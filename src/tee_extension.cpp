#include "tee_extension.hpp"
#include "tee_logical.hpp"
#include "tee_parser.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parser_extension.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction tee_function("tee", {LogicalType::TABLE}, nullptr, nullptr);
	tee_function.bind_operator = LogicalTee::TeeBindOperator;
	tee_function.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("path", LogicalType::VARCHAR)
		    .Add("symbol", LogicalType::VARCHAR)
		    .Add("terminal", LogicalType::BOOLEAN)
		    .Add("table_name", LogicalType::VARCHAR)
		    .Add("pager", LogicalType::BOOLEAN)
		    .Add("maxrows", LogicalType::BIGINT)
		    .Alias("max_rows");
	});
	loader.RegisterFunction(tee_function);

	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	// parser is disabled by default, if a user want to use it he can set:
	// SET allow_parser_override_extension = 'fallback';

	// config.SetOptionByName("allow_parser_override_extension", Value("fallback"));

	ParserExtension parser_extension;
	parser_extension.parser_override = TeeParserExtension::ParserOverrideFunction;
	ParserExtension::Register(config, std::move(parser_extension));
}

void TeeExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(tee, loader) {
	duckdb::LoadInternal(loader);
}
}
