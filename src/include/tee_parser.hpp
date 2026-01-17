#pragma once

#include "duckdb/main/database.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

struct TeeParseData : public ParserExtensionParseData {
	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq<TeeParseData>();
	}
	string ToString() const override {
		return "Parsed Data";
	}
};

struct TeeParserInfo : public ParserExtensionInfo {};

class TeeParserExtension : public ParserExtension {
public:
	static ParserOverrideResult ParserOverrideFunction(ParserExtensionInfo *info, const string &query);
};

}; // namespace duckdb