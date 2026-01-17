#include "tee_operator.hpp"
#include "tee_extension.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

PhysicalTeeOperator::PhysicalTeeOperator(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                         idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::TABLE_SCAN, std::move(types), estimated_cardinality) {
}

PhysicalTeeOperator::~PhysicalTeeOperator() {
}

unique_ptr<OperatorState> PhysicalTeeOperator::GetOperatorState(ExecutionContext &) const {
	return make_uniq<OperatorState>();
}

OperatorResultType PhysicalTeeOperator::Execute(ExecutionContext &, DataChunk &input, DataChunk &output,
                                                GlobalOperatorState &, OperatorState &) const {
	output.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalTeeOperator::GetName() const {
	return "tee";
}

} // namespace duckdb