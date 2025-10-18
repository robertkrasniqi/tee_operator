#include "include/tee_parser.hpp"
#include "include/tee_extension.hpp"


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
	ParserExtension tee_parser;

	tee_parser.parser_info = make_shared_ptr<TeeParserInfo>();
	tee_parser.parse_function = TeeParserExtension::ParseFunction;
	tee_parser.plan_function = TeeParserExtension::PlanFunction;

	db.instance->config.parser_extensions.push_back(tee_parser);

	std::cout << "\n" << "Debugging: RegisterParserExtension was called." << "\n";
}

};