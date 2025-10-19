#include "include/tee_parser.hpp"
#include "include/tee_extension.hpp"
#include "duckdb/parser/parser.hpp"


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

// dont work yet
// This function gets registered at the beginning but is never called again
ParserOverrideResult TeeParserExtension::ParserOverrideFunction(ParserExtensionInfo *info, const string &query) {
	std::cout << "Reached ParserOverrideFunction!" << "\n";
	if (true) {
		auto modified_query = "SELECT * FROM tee((SELECT * FROM RANGE (10)));";
		Parser parser;
		parser.ParseQuery(modified_query);
		auto &statements = parser.statements;
		vector<unique_ptr<SQLStatement>> result_statements;
		for (auto &stmt : statements) {
			result_statements.push_back(std::move(stmt));
		}
		return ParserOverrideResult(std::move(result_statements));

	}
	ParserOverrideResult result;
	return result;
}

//! ParserFunction is called by DuckDB for every query string
//! After that gets passed back to regular parser
ParserExtensionParseResult TeeParserExtension::ParseFunction(ParserExtensionInfo *info, const string &query) {
	// Debugging
	std::cout << "Query at TeeParserExtension::ParseFunction: " << query << "\n" << "\n";

	// TODO: Implement parse logic here
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