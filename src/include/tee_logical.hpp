#pragma once

#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/subquery/flatten_dependent_join.hpp"

namespace duckdb {

class LogicalTee : public LogicalExtensionOperator {
public:
	LogicalTee(TableIndex table_index, named_parameter_map_t tee_named_parameters);

	TableIndex table_index;
	named_parameter_map_t tee_named_parameters;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;

	// passes childs through unchanged
	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

	// Correlation hook
	vector<ColumnBinding> PushdownDependentJoin(FlattenDependentJoins &flattener, unique_ptr<LogicalOperator> &plan,
	                                            bool propagate_null_values, vector<ColumnBinding> column_bindings,
	                                            BindingReplacementGraph &replacement_graph) override;

	bool SupportSerialization() const override {
		return false;
	}

	string GetExtensionName() const override {
		return "logical_tee";
	}

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
