#include "include/tee_physical.hpp"

namespace duckdb {

PhysicalTee::PhysicalTee(PhysicalPlan &physical_plan, vector<LogicalType> types_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality) {
}

OperatorResultType PhysicalTee::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                        GlobalOperatorState &global_state, OperatorState &state) const {
	// Only passthrough
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}
} // namespace duckdb
