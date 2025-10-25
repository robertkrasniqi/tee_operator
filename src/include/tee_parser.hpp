#pragma once

#include "duckdb/main/database.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

struct TeeParserInfo : public ParserExtensionInfo {};

class TeeParserExtension : public ParserExtension {
public:
	static ParserOverrideResult ParserOverrideFunction(ParserExtensionInfo *info, const string &query);
	static ParserExtensionParseResult ParseFunction(ParserExtensionInfo *info, const string &query);
	static ParserExtensionPlanResult PlanFunction(ParserExtensionInfo *info, ClientContext &context,
												  unique_ptr<ParserExtensionParseData> parse_data);
};


};



