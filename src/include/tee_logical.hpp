#pragma once

#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/column_binding.hpp"

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

	bool SupportsDecorrelation() const override {
		return true;
	}

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
