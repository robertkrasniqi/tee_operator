#pragma once

#include "tee_physical.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

//! Carries the named parameters and passes the child through unchanged
class LogicalTee : public LogicalExtensionOperator {
public:
	explicit LogicalTee(named_parameter_map_t tee_named_parameters_p)
	    : tee_named_parameters(std::move(tee_named_parameters_p)) {
	}

	named_parameter_map_t tee_named_parameters;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		D_ASSERT(children.size() == 1);

		vector<string> names;
		names.reserve(types.size());
		if (children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			// read and reuse the child column names
			for (const auto &expr : children[0]->expressions) {
				names.push_back(expr->GetAlias().GetIdentifierName());
			}
		} else {
			// provide column names for empty results
			// example:
			// SELECT * FROM tee((SELECT 1 AS a WHERE false));
			for (idx_t i = 0; i < types.size(); i++) {
				names.push_back("col" + to_string(i));
			}
		}
		D_ASSERT(names.size() == types.size());

		auto &child = planner.CreatePlan(*children[0]);
		auto &physical_tee = planner.Make<PhysicalTee>(types, names, estimated_cardinality, tee_named_parameters);
		physical_tee.children.push_back(child);

		return physical_tee;
	}

	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

	bool SupportsDecorrelation() const override {
		return true;
	}

	// Create a projection (below Tee) with the input columns
	static unique_ptr<LogicalOperator> TeeBindOperator(ClientContext &context, TableFunctionBindInput &input,
	                                                   TableIndex table_bind_index, vector<Identifier> &return_names) {
		return_names = input.input_table_names;

		// apply the alias list by ourselves since this bind_operator returns before the binder would do it
		const auto &column_alias = input.ref.column_name_alias;
		for (idx_t i = 0; i < column_alias.size() && i < return_names.size(); i++) {
			return_names[i] = column_alias[i];
		}

		auto &child = *input.input_plan;
		auto child_bindings = child->GetColumnBindings();

		D_ASSERT(child_bindings.size() == input.input_table_types.size());

		vector<unique_ptr<Expression>> select_list;
		select_list.reserve(child_bindings.size());

		// prepare columns for the projection
		for (idx_t i = 0; i < child_bindings.size(); i++) {
			select_list.push_back(
			    make_uniq<BoundColumnRefExpression>(return_names[i], input.input_table_types[i], child_bindings[i]));
		}

		auto projection = make_uniq<LogicalProjection>(table_bind_index, std::move(select_list));
		projection->children.push_back(std::move(child));

		auto logical_tee = make_uniq<LogicalTee>(input.named_parameters);
		logical_tee->children.push_back(std::move(projection));

		return std::move(logical_tee);
	}

	string GetExtensionName() const override {
		return "logical_tee";
	}

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};

} // namespace duckdb
