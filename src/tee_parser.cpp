#include "include/tee_parser.hpp"
#include "include/tee_extension.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/string_util.hpp"


namespace duckdb {

//! Holds the data of successfully parse step
struct TeeParseData : public ParserExtensionParseData {
	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq<TeeParseData>();
	}//! can be used later for debugging
	string ToString() const override {
		return "Parsed Data";
	}
};

static string CustomTeeParser(const string &query) {
	// TODO´s
	// -- handle multiple tee calls
	// -- edge cases, invalid input ..
	// -- handel spaces, e.g. "tee ("

	// for later we want to know how many function calls we have
	u_int8_t tee_occurs = 0;

	string result_query = StringUtil::Lower(query);

	string pattern = "tee(";

	// for now check for tee(
	// but should check for tee( && not tee((
	int pos_tee = result_query.find(pattern);
	result_query.insert(pos_tee + pattern.size(), "(");


	std::stack<char> paranthese_stack;

	paranthese_stack.push('(');

	for (idx_t i = pos_tee + pattern.size() + 1; i < result_query.size(); i++) {
		if (result_query[i] == '(') {
			paranthese_stack.push('(');
		}
		if (result_query[i] == ')') {
			paranthese_stack.pop();
		}
		if (paranthese_stack.empty()) {
			if (i + 1 == result_query.size()) {
				result_query.push_back(')');
				break;
			}
			result_query.insert(i + 1, ")");
			break;
		}
	}
	return result_query;
}

//! Is called for parsing
//! Makes it possible to use a custom parser
ParserOverrideResult TeeParserExtension::ParserOverrideFunction(ParserExtensionInfo *info, const string &query) {

	// no "tee" in query, return to default parser
	if (!StringUtil::Contains(StringUtil::Lower(query), "tee")) {
		return ParserOverrideResult();
	}

	// call own parser
	string modified_query = CustomTeeParser(query);

	// maybe change the try logic later
	try {
		Parser parser;
		parser.ParseQuery(modified_query);

		auto &statements = parser.statements;
		vector<unique_ptr<SQLStatement>> result_statements;
		for (auto &stmt : statements) {
			result_statements.push_back(std::move(stmt));
		}

		// This occurs for a valid query input
		std::cout << "Debug: Parsing successful in TeeParserExtension.\n";

		return ParserOverrideResult(std::move(result_statements));
	}
	catch (ParserException &ex) {
		// what() prints additional information, e.g. the position where the error is
		// Later I want to parse the things before the error, and handle (parse correct) the things from the error on
		std::cout << "Exception message:\n" << ex.what() << "\n";

		string errorMessage = ex.what();

		/*  search for position in errorMessage and extract the index
		 * {"exception_type":"Parser","exception_message":"syntax error at or near \"SELECT\"","position":"18","error_subtype":"SYNTAX_ERROR"}
		 *																								  ^  ^
		*/

		string leftIndexStr = "\"position\":\"";
		size_t leftPartIdx = errorMessage.find(leftIndexStr) + leftIndexStr.length();
		size_t rightPartIdx = errorMessage.find(",\"error_subtype");

		string idxErrorTemp = errorMessage.substr(leftPartIdx, (rightPartIdx - 1)  - leftPartIdx);

		// convert string to int
		int64_t idxParseError = std::stoll(idxErrorTemp);
		std::cout << idxParseError << "\n";
		CustomTeeParser(query.substr(idxParseError, query.size()));

		return ParserOverrideResult();
	}
}


ParserExtensionParseResult TeeParserExtension::ParseFunction(ParserExtensionInfo *info, const string &query) {
	// Dummyfunction by now
	ParserExtensionParseResult result;
	result.parse_data = make_uniq<TeeParseData>();
	return result;
}

//! PlanFunction is called after parsing
ParserExtensionPlanResult TeeParserExtension::PlanFunction(ParserExtensionInfo *info, ClientContext &context,
														   unique_ptr<ParserExtensionParseData> parse_data) {
	// TODO: Implement plan logic. (for logical plan I guess)
	ParserExtensionPlanResult result;
	return result;
}

};