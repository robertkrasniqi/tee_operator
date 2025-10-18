#pragma once

#include "tee_extension.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "tee_parser.hpp"

namespace duckdb {

// tbd
struct TeeParserInfo : public ParserExtensionInfo {};

class TeeParserExtension {
public:
	static ParserExtensionParseResult ParseFunction(ParserExtensionInfo *info, const string &query);
	static ParserExtensionPlanResult PlanFunction(ParserExtensionInfo *info, ClientContext &context,
												  unique_ptr<ParserExtensionParseData> parse_data);
};

// load parser
void RegisterParserExtension(DuckDB &db);

};



