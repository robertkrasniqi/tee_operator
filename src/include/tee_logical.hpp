#pragma once

#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/common/projection_index.hpp"
#include "duckdb/planner/subquery/flatten_dependent_join.hpp"

namespace duckdb {

class LogicalTee : public LogicalExtensionOperator {
public:
	LogicalTee(TableIndex table_index, vector<LogicalType> output_types, vector<string> output_names,
	           named_parameter_map_t tee_named_parameters);

	TableIndex table_index;
	vector<LogicalType> types_output;
	vector<string> names_output;
	vector<column_t> projected_input;
	named_parameter_map_t tee_named_parameters;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;

	vector<ColumnBinding> GetColumnBindings() override;

	// Correlation hook
	vector<ColumnBinding> PushdownDependentJoin(FlattenDependentJoins &flattener, unique_ptr<LogicalOperator> &plan,
	                                            bool propagate_null_values,
	                                            vector<ColumnBinding> column_bindings) override;

	string GetExtensionName() const override {
		return "logical_tee";
	}

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
