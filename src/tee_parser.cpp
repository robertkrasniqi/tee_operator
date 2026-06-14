#include "include/tee_parser.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/string_util.hpp"
#include <fmt/core.h>
#include <string>
#include "format"
#include <iostream>

namespace duckdb {

// Returns true if char is alphanumeric
static bool IsAlNum(const char c) {
	return isalnum(static_cast<unsigned char>(c)) || c == '_';
}

// Find the next tee invocation
static idx_t FindTeeCall(const string &query, idx_t pos) {
	string lower = StringUtil::Lower(query);
	while (pos < lower.size()) {
		idx_t tee_pos = lower.find("tee", pos);
		if (tee_pos == string::npos) {
			return string::npos;
		}
		// Boundary before
		if (tee_pos > 0 && IsAlNum(lower[tee_pos - 1])) {
			pos = tee_pos + 3;
			continue;
		}
		// Boundary after
		if (tee_pos + 3 < lower.size() && IsAlNum(lower[tee_pos + 3])) {
			pos = tee_pos + 3;
			continue;
		}

		idx_t paren_pos = tee_pos + 3;
		// Ignore whitespace after first '('
		while (paren_pos < lower.size() && lower[paren_pos] == ' ') {
			paren_pos++;
		}
		if (paren_pos >= lower.size() || lower[paren_pos] != '(') {
			pos = tee_pos + 3;
			continue;
		}
		return tee_pos;
	}
	return string::npos;
}

// Rewrites every tee(...) invocation into tee((...))
static string RewriteTee(const string &query) {
	string result_query = query;
	idx_t pos = 0;

	while (true) {
		idx_t tee_pos = FindTeeCall(result_query, pos);
		if (tee_pos == string::npos) {
			break;
		}

		// Locate first '('
		idx_t pos_outer_paren = tee_pos + 3;
		while (result_query[pos_outer_paren] == ' ') {
			pos_outer_paren++;
		}

		// If we already got 'tee((...))' - skip
		idx_t pos_inner_paren = pos_outer_paren + 1;
		while (pos_inner_paren < result_query.size() && result_query[pos_inner_paren] == ' ') {
			pos_inner_paren++;
		}
		if (pos_inner_paren < result_query.size() && result_query[pos_inner_paren] == '(') {
			pos = pos_outer_paren + 1;
			continue;
		}

		// Insert extra '('
		result_query.insert(pos_outer_paren, "(");

		// Search pos to insert matching ')'
		// Skip strings inside the query
		idx_t depth = 1;
		bool inside_string = false;
		char string_char = 0;
		idx_t i = pos_outer_paren + 2;

		while (i < result_query.size()) {
			char c = result_query[i];
			if (inside_string) {
				if (c == string_char) {
					inside_string = false;
				}
				i++;
				continue;
			}
			if (c == '\'' || c == '"') {
				inside_string = true;
				string_char = c;
			} else if (c == '(') {
				depth++;
			} else if (c == ')') {
				depth--;
				if (depth == 0) {
					result_query.insert(i + 1, ")");
					i++;
					break;
				}
			}
			i++;
		}
		pos = i + 1;
	}
	return result_query;
}

ParserOverrideResult TeeParserExtension::ParserOverrideFunction(ParserExtensionInfo *info, const string &query,
                                                                ParserOptions &options) {
	if (FindTeeCall(query, 0) == string::npos) {
		return ParserOverrideResult();
	}

	string modified_query = RewriteTee(query);

	Parser parser;
	parser.ParseQuery(modified_query);

	vector<unique_ptr<SQLStatement>> result_statements;
	for (auto &stmt : parser.statements) {
		result_statements.push_back(std::move(stmt));
	}
	return ParserOverrideResult(std::move(result_statements));
}
}; // namespace duckdb