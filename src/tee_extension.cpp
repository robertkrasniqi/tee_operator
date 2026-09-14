#include "tee_extension.hpp"
#include "tee_logical.hpp"
#include "tee_physical.hpp"
#include "tee_parser.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

static unique_ptr<LogicalOperator> TeeBindOperator(ClientContext &context, TableFunctionBindInput &input,
                                                   TableIndex table_bind_index, vector<Identifier> &return_names) {

	return_names = input.input_table_names;

	auto &child = *input.input_plan;
	auto child_bindings = child->GetColumnBindings();
	D_ASSERT(child_bindings.size() == input.input_table_types.size());

	vector<unique_ptr<Expression>> select_list;
	select_list.reserve(child_bindings.size());
	for (idx_t i = 0; i < child_bindings.size(); i++) {
		auto expr = make_uniq<BoundColumnRefExpression>(input.input_table_types[i], child_bindings[i]);
		expr->SetAlias(input.input_table_names[i]);
		select_list.push_back(std::move(expr));
	}

	auto projection = make_uniq<LogicalProjection>(table_bind_index, std::move(select_list));
	projection->children.push_back(std::move(child));

	auto logical_tee = make_uniq<LogicalTee>(table_bind_index, input.named_parameters);
	logical_tee->children.push_back(std::move(projection));

	return std::move(logical_tee);
}

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction tee_function("tee", {LogicalType::TABLE}, nullptr, nullptr);
	tee_function.bind_operator = TeeBindOperator;
	tee_function.named_parameters["path"] = LogicalType::VARCHAR;
	tee_function.named_parameters["symbol"] = LogicalType::VARCHAR;
	tee_function.named_parameters["terminal"] = LogicalType::BOOLEAN;
	tee_function.named_parameters["table_name"] = LogicalType::VARCHAR;
	tee_function.named_parameters["pager"] = LogicalType::BOOLEAN;
	tee_function.named_parameters["maxrows"] = LogicalType::BIGINT;
	tee_function.named_parameters["force_materialize"] = LogicalType::BOOLEAN;
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