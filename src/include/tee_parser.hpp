#pragma once

#include "tee_extension.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

class  TeeParser{
public:
	//! register the parser
	static void RegisterParserExtension(DuckDB &db) {};

private:
	//! TBD / set staticleinfo
	struct TeeParserInfo : public ParserExtensionInfo {};

	//! The ParserExtensionParseData holds the result of a successful parse step
	//! It will be passed along to the subsequent plan function
	struct TeeParseData : public ParserExtensionParseData {
		unique_ptr<ParserExtensionParseData> Copy() const override;
		string ToString() const override;
	};
	//! ParseFunction
	static ParserExtensionParseResult ParseFunction(ParserExtensionInfo *info, const string &query);
	//! PlanFunction
	static ParserExtensionPlanResult PlanFunction(ParserExtensionInfo *info, ClientContext &context,
													  unique_ptr<ParserExtensionParseData> parse_data);
	};
};



