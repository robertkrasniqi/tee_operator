#include "include/tee_parser.hpp"
#include "include/tee_extension.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/string_util.hpp"

#include <iostream>

#define PRINT_QUERY

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

// Helper function, concatenate input query with named parameters if present
static string ConcatNamedParams(const string &input_cte_name, const string &named_params) {
	string rewrite_call = "__rewrite_query((FROM " + input_cte_name + ")";
	if (!named_params.empty()) {
		rewrite_call += ", " + named_params;
	}
	rewrite_call += ")";
	return rewrite_call;
}

// RewriteTeeQuery rewrites the tee call such that it is forced to be materialized
// Example:
/*
SELECT * FROM tee((SELECT i FROM range(3) AS t(i)), terminal = true);
                  └────────────┬──────────────┘     └────────┬──────┘
(                              │                             │
SELECT *                       │                             └──────┐
FROM (                         └──────────────────┐					│
    SELECT *								      │					│
    FROM [									      │					│
        WITH __rewrite_input_0 AS MATERIALIZED (  │					│
            SELECT i FROM range(3) AS t(i) ───────┘					│
        ),															│
        __rewrite_force_0 AS (										│
            SELECT count(*) AS __rewrite_consumed_0		  ┌─────────┴──────┐
            FROM __rewrite_query((FROM __rewrite_input_0), terminal = true)
        )
        SELECT __rewrite_input_0.*
        FROM __rewrite_input_0, __rewrite_force_0
        ] AS __rewrite_result_0
)
*/
static string RewriteTeeQuery(const string &input_subquery, const string &named_params, idx_t tee_call_idx) {
	// Contructed CTE name for the original tee input
	const string input_cte_name = "__rewrite_input_" + to_string(tee_call_idx);
	// Contructed CTE name for the forced full scan
	const string force_cte_name = "__rewrite_force_" + to_string(tee_call_idx);
	// Alias for final query
	const string result_alias = "__rewrite_result_" + to_string(tee_call_idx);

	// rewrite_call holds alias "__rewrite_query" for the tee function call and named parameter if present
	// __rewrite_query is the actually registered function name
	const string rewrite_call = ConcatNamedParams(input_cte_name, named_params);

	string result;

	result += "\n(SELECT * FROM ( \n \t";
	result += "WITH " + input_cte_name + " AS MATERIALIZED (\n \t \t";
	result += input_subquery + "\n \t \t";
	result += "), " + force_cte_name + " AS ( \n \t \t";
	result += "SELECT count(*) FROM " + rewrite_call + "\n \t \t \t";
	result += ") \n \t \t \t";
	result += "SELECT " + input_cte_name + ".* \n \t";
	result += "FROM " + input_cte_name + ", " + force_cte_name + "\n";
	result += ") AS " + result_alias + ")\n";

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
	std::cout << "Tee Parser: BuildTeeQuery(query) called \n";
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