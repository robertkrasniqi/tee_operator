#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalTee : public PhysicalOperator {
public:
	PhysicalTee(PhysicalPlan &physical_plan, vector<LogicalType> types, idx_t estimated_cardinality);

	string GetName() const override {
		return "physical_tee";
	}

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &global_state, OperatorState &state) const override;
};

} // namespace duckdb