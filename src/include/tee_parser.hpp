#pragma once

#include "duckdb/parser/parser_extension.hpp"

namespace duckdb {
class TeeParserExtension {
public:
	static ParserOverrideResult ParserOverrideFunction(ParserExtensionInfo *info, const string &query,
	                                                   ParserOptions &options);
};
} // namespace duckdb
