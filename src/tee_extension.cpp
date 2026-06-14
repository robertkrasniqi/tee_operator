#include "tee_extension.hpp"
#include "tee_logical.hpp"
#include "tee_physical.hpp"
#include "tee_parser.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

// DuckDB Binder validates during query planning that every table-in-out function
// has an actual in_out_function
static OperatorResultType TeeDummyInOut(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                        DataChunk &output) {
	throw InternalException("TeeDummyInOut called - Should not happen");
}

static unique_ptr<FunctionData> TeeBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	return_types = input.input_table_types;
	names = input.input_table_names;
	return make_uniq<TeeBindData>(names, return_types, input.named_parameters);
}

// DuckDB creates LogicalGet nodes which we replace recursively with tee nodes
static void ReplaceTeeNodes(unique_ptr<LogicalOperator> &node) {
	for (auto &child : node->children) {
		ReplaceTeeNodes(child);
	}
	if (node->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = node->Cast<LogicalGet>();
	if (get.function.name != "tee") {
		return;
	}
	auto logical_tee = make_uniq<LogicalTee>(get.table_index, get.returned_types, get.names);

	logical_tee->projected_input = get.projected_input;

	for (auto &child : get.children) {
		logical_tee->children.push_back(std::move(child));
	}

	node = std::move(logical_tee);
}

// Extension Optimizer - Gets called after DuckDB optimizer
static void TeeOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	ReplaceTeeNodes(plan);
}

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction tee_function("tee", {LogicalType::TABLE}, nullptr, TeeBind);
	tee_function.in_out_function = TeeDummyInOut;
	tee_function.named_parameters["path"] = LogicalType::VARCHAR;
	tee_function.named_parameters["symbol"] = LogicalType::VARCHAR;
	tee_function.named_parameters["terminal"] = LogicalType::BOOLEAN;
	tee_function.named_parameters["table_name"] = LogicalType::VARCHAR;
	tee_function.named_parameters["pager"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(tee_function);

	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	OptimizerExtension tee_optimizer;
	tee_optimizer.optimize_function = TeeOptimize;
	OptimizerExtension::Register(config, std::move(tee_optimizer));

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