#include "include/tee_parser.hpp"

#include "duckdb/parser/parser.hpp"
#include "duckdb/common/string_util.hpp"
#include <fmt/core.h>
#include <string>

#include "format"

#include <iostream>

//#define PRINT_QUERY

namespace duckdb {

// Prevents matching "tee" inside other identifiers
// Checks character before and after tee
// Return false if a letter, digit or underscore before/after tee, else true
static bool IsStandAloneTee(const string &query, idx_t idx) {
	if (idx >= query.size()) {
		return true;
	}
	const char c = query[idx];
	return !(StringUtil::CharacterIsAlpha(c) || StringUtil::CharacterIsDigit(c) || c == '_');
}

static idx_t SkipWhitespace(const string &query, idx_t idx) {
	while (idx < query.size() && StringUtil::CharacterIsSpace(query[idx])) {
		idx++;
	}
	return idx;
}

static idx_t FindMatchingParentheses(const string &query, idx_t idx) {
	idx_t parens_counter = 0;
	for (idx_t i = idx; i < query.size(); i++) {
		if (query[i] == '(') {
			parens_counter++;
		} else if (query[i] == ')') {
			parens_counter--;
			if (parens_counter == 0) {
				return i;
			}
		}
	}
	// No matching closing parentheses was found
	return DConstants::INVALID_INDEX;
}

// Helper function, concatenate input query with named paramseters if present
static string ConcatNamedParams(const string &input_subquery, const string &named_params) {
	string rewrite_call = "__rewrite_query((";
	rewrite_call += input_subquery;
	rewrite_call += ")";
	if (!named_params.empty()) {
		rewrite_call += ", " + named_params;
	}
	rewrite_call += ")";
	return rewrite_call;
}

// RewriteTeeQuery rewrites the tee call such that it is forced to consume all rows
static string RewriteTeeQuery(const string &input_subquery, const string &named_params, idx_t tee_call_idx) {
	const string rewrite_call = ConcatNamedParams(input_subquery, named_params);

	string result;

	result += "\n(SELECT __rewrite_table.* EXCLUDE (__rewrite_dummy_counter)\n";
	result += "FROM (\n";
	result += "\tSELECT\n";
	result += "\t\t__rewrite_select.*, \n";
	result += "\t\tcount(*) OVER () AS __rewrite_dummy_counter\n";
	result += "\tFROM " + rewrite_call + " AS __rewrite_select\n";
	result += ") AS __rewrite_table)\n";

#ifdef PRINT_QUERY
	result += "\n";
	Printer::Print(result);
#endif
	return result;
}

static string BuildTeeQuery(const string &query) {
	string result;

	idx_t query_idx = 0;
	idx_t tee_index = 0;

	while (query_idx < query.size()) {
		// check if we found tee
		if (query_idx + 3 <= query.size() && StringUtil::CIEquals(query.substr(query_idx, 3), "tee") &&
		    (query_idx == 0 || IsStandAloneTee(query, query_idx - 1)) && IsStandAloneTee(query, query_idx + 3)) {
			// Skip whitespaces
			auto tee_query_idx = SkipWhitespace(query, query_idx + 3);
			if (tee_query_idx >= query.size() || query[tee_query_idx] != '(') {
				// no function call - skip
				result += query[query_idx++];
				continue;
			}
			// Found "tee(" - now parse its arguments
			auto outer_open = tee_query_idx;
			// First char after '('
			auto tee_query_start_idx = SkipWhitespace(query, outer_open + 1);
			auto outer_close = FindMatchingParentheses(query, outer_open);

			string tee_query;
			string named_params;

			// Case 1: Double parenthesis: tee((subquery), named params..)
			if (tee_query_start_idx < query.size() && query[tee_query_start_idx] == '(') {
				auto inner_first_char_idx = tee_query_start_idx;
				auto inner_last_char_idx = FindMatchingParentheses(query, inner_first_char_idx);
				// Extract the subquery between inner parens
				tee_query = query.substr(inner_first_char_idx + 1, inner_last_char_idx - inner_first_char_idx - 1);
				// Check if there's anything after the inner closing paren
				auto after_inner = SkipWhitespace(query, inner_last_char_idx + 1);
				if (after_inner < outer_close) {
					if (query[after_inner] != ',') {
						// Invalid - has to begin with "," i.e., the named params
						result += query[query_idx++];
						continue;
					}
					// Everything after the comma are named parameters
					named_params = query.substr(after_inner + 1, outer_close - after_inner - 1);
					StringUtil::Trim(named_params);
				}
			} else {
				// Case 2: Single parenthesis tee(SELECT ...)
				tee_query = query.substr(tee_query_start_idx, outer_close - tee_query_start_idx);
				StringUtil::Trim(tee_query);
			}

			// Rewrite tee into appropriate syntax
			auto rewritten_query = RewriteTeeQuery(tee_query, named_params, tee_index);
			tee_index++;
			result += rewritten_query;
			// Skip past the tee call and search for the next
			query_idx = outer_close + 1;
			continue;
		}
		// No tee call yet -> copy current char to result query
		result += query[query_idx++];
	}
	return result;
}

ParserOverrideResult TeeParserExtension::ParserOverrideFunction(ParserExtensionInfo *info, const string &query,
                                                                ParserOptions &options) {
	// No tee -> skip
	if (!StringUtil::Contains(StringUtil::Lower(query), "tee")) {
		return ParserOverrideResult();
	}

	// Rewrite all tee calls in query
	string modified_query = BuildTeeQuery(query);
	if (modified_query == query) {
		return ParserOverrideResult();
	}

	try {
		// Parse the new query using DuckDBs normal parser
		Parser parser(options);
		parser.ParseQuery(modified_query);
		auto &statements = parser.statements;
		vector<unique_ptr<SQLStatement>> result_statements;
		for (auto &stmt : statements) {
			result_statements.push_back(std::move(stmt));
		}
		return ParserOverrideResult(std::move(result_statements));
	} catch (ParserException &ex) {
		return ParserOverrideResult(ex);
	}
}

}; // namespace duckdb