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
	// our example test query:
	// SELECT * FROM tee(SELECT * FROM (VALUES (1,2))) _(a,b) WHERE a < 1;

	// I thought about some hardcoded replacement here, something like regex pattern matching
	// I am aware that sql syntax can be very complicated, but if it´s possible to solve this problem with only one function
	// it should be the best approach
	return query;
}

//! Is called for parsing
//! Makes it possible to use a custom parser
ParserOverrideResult TeeParserExtension::ParserOverrideFunction(ParserExtensionInfo *info, const string &query) {

	std::cout << "Debug: We are inside the ParserOverrideFunction" << "\n";

	// no "tee" in query -> pass back to default parser
	if (!StringUtil::Contains(query, "tee")) {
		return ParserOverrideResult();
	}

	// This is for testing only!
	//
	// Run query:
	// SELECT * FROM tee(SELECT * FROM (VALUES (1,2))) _(a,b) WHERE a < 1;
	//
	// Yet this is hardcoded. It is only intended to demonstrate that
	// this is a way how we can manipulate queries on the parser level
	if (query == "SELECT * FROM tee(SELECT * FROM (VALUES (1,2))) _(a,b) WHERE a < 1;") {
		string correct_query = "SELECT * FROM tee((SELECT * FROM (VALUES (1,2)))) _(a,b) WHERE a < 1;";

		Parser parser;
		parser.ParseQuery(correct_query);

		auto &statements = parser.statements;
		vector<unique_ptr<SQLStatement>> result_statements;
		for (auto &stmt : statements) {
			result_statements.push_back(std::move(stmt));
		}
		return ParserOverrideResult(std::move(result_statements));
	}

	// call own parser
	// just return the input by now
	string modified_query = CustomTeeParser(query);

	Parser parser;
	parser.ParseQuery(modified_query);

	auto &statements = parser.statements;
	vector<unique_ptr<SQLStatement>> result_statements;
	for (auto &stmt : statements) {
		result_statements.push_back(std::move(stmt));
	}
	return ParserOverrideResult(std::move(result_statements));
}

//! ParserFunction is called by DuckDB for every query string
//! After that gets passed back to regular parser
ParserExtensionParseResult TeeParserExtension::ParseFunction(ParserExtensionInfo *info, const string &query) {
	// Debugging
	std::cout << "Query at TeeParserExtension::ParseFunction: " << query << "\n" << "\n";

	ParserExtensionParseResult result;
	result.parse_data = make_uniq<TeeParseData>();
	// return parse result
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