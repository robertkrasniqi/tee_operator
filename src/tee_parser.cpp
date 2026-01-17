#include "include/tee_parser.hpp"
#include "include/tee_extension.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

// Parses a SQL-Query and rewrites all occurrences of tee(..) by
// inserting an extra pair of parentheses
// Example
//		Input:	SELECT * FROM  tee(TABLE t)
//      Output: SELECT * FROM tee((TABLE t)
static string CustomTeeParser(const string &query) {
	string result_query = StringUtil::Lower(query);
	// pos_begin is the index we start at for each new tee call
	idx_t pos_begin = 0;
	while (true) {
		idx_t pos_tee = result_query.find("tee", pos_begin);
		// no tee's left -> done
		if (pos_tee == string::npos) {
			break;
		}
		// add 3 positions after we found tee
		idx_t pos_curr = pos_tee + 3;
		// skip spaces till first no space character
		while (pos_curr < result_query.size() && StringUtil::CharacterIsSpace(result_query[pos_curr])) {
			pos_curr++;
		}
		// insert the first '(' and use a stack to find place where last closing parentheses belong to
		result_query.insert(pos_curr, "(");
		std::stack<char> parenthesis_stack;
		parenthesis_stack.push('(');
		for (pos_curr = pos_curr + 2; pos_curr < result_query.size(); pos_curr++) {
			if (result_query[pos_curr] == '(') {
				parenthesis_stack.push('(');
			} else if (result_query[pos_curr] == ')') {
				parenthesis_stack.pop();
				if (pos_curr + 1 == result_query.size()) {
					result_query.push_back(')');
				} else {
					result_query.insert(pos_curr + 1, ")");
				}
				break;
			}
		}
		pos_begin = pos_curr;
	}
#ifdef DEBUG
	std::cout << "\n" << "Parsed: " << result_query << "\n \n";
#endif
	return result_query;
}

ParserOverrideResult TeeParserExtension::ParserOverrideFunction(ParserExtensionInfo *info, const string &query) {
	// if there are no tee-calls, return to DuckDB parser
	if (!StringUtil::Contains(StringUtil::Lower(query), " tee")) {
		return ParserOverrideResult();
	}
	// insert extra parentheses after each tee call
	string modified_query = CustomTeeParser(query);
	try {
		// after inserting parentheses, DuckDB should always be able to parse it normally
		Parser parser;
		parser.ParseQuery(modified_query);
		auto &statements = parser.statements;
		vector<unique_ptr<SQLStatement>> result_statements;
		for (auto &stmt : statements) {
			result_statements.push_back(std::move(stmt));
		}
		return ParserOverrideResult(std::move(result_statements));
	} catch (ParserException &ex) {
#ifdef DEBUG
		string errorMessage = ex.what();
		string leftIndexStr = "\"position\":\"";
		size_t leftPartIdx = errorMessage.find(leftIndexStr) + leftIndexStr.length();
		size_t rightPartIdx = errorMessage.find(",\"error_subtype");
		string idxErrorTemp = errorMessage.substr(leftPartIdx, (rightPartIdx - 1) - leftPartIdx);
		// convert string to int
		int64_t idxParseError = std::stoll(idxErrorTemp);
		std::cout << idxParseError << "\n";
#endif
		return ParserOverrideResult();
	}
}

}; // namespace duckdb