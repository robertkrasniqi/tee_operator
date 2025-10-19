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

void RegisterParserExtension(DuckDB &db) {
	TeeParserExtension tee_parser;

	tee_parser.parser_override = TeeParserExtension::ParserOverrideFunction;
	tee_parser.parser_info = make_shared_ptr<TeeParserInfo>();
	tee_parser.parse_function = TeeParserExtension::ParseFunction;
	tee_parser.plan_function = TeeParserExtension::PlanFunction;

	db.instance->config.parser_extensions.push_back(tee_parser);

	std::cout << "\n" << "Debugging: RegisterParserExtension was called." << "\n";
}

};