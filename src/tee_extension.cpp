#include "tee_extension.hpp"
#include "tee_logical.hpp"
#include "tee_parser.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parser_extension.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction tee_function("tee", {LogicalType::TABLE}, nullptr, nullptr);
	tee_function.bind_operator = LogicalTee::TeeBindOperator;
	tee_function.named_parameters["path"] = LogicalType::VARCHAR;
	tee_function.named_parameters["symbol"] = LogicalType::VARCHAR;
	tee_function.named_parameters["terminal"] = LogicalType::BOOLEAN;
	tee_function.named_parameters["table_name"] = LogicalType::VARCHAR;
	tee_function.named_parameters["pager"] = LogicalType::BOOLEAN;
	tee_function.named_parameters["maxrows"] = LogicalType::BIGINT;
	loader.RegisterFunction(tee_function);

	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.SetOptionByName("allow_parser_override_extension", Value("fallback"));

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