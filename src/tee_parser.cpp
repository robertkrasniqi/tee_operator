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
	// TODO: Implement the function
	return query;
}

//! Is called for parsing
//! Makes it possible to use a custom parser
ParserOverrideResult TeeParserExtension::ParserOverrideFunction(ParserExtensionInfo *info, const string &query) {

	std::cout << "Debug: We are inside the ParserOverrideFunction" << "\n";

	// no "tee" in query, return to default parser
	if (!StringUtil::Contains(query, "tee")) {
		return ParserOverrideResult();
	}

	// call own parser
	// just return the input by now
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
		std::cout << "Debug: Caught ParserException in TeeParserExtension!\n";
		// what() prints additional information, e.g. the position where the error is
		// Later I want to parse the things before the error, and handle (parse correct) the things from the error on
		std::cout << "Exception message:\n" << ex.what() << "\n";
		// GetStackTrace() prints Stack
		std::cout << "Stacktrace:\n" << Exception::GetStackTrace() <<  "\n";

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