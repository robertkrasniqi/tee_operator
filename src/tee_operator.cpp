#include "tee_operator.hpp"
#include "tee_extension.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

PhysicalTeeOperator::PhysicalTeeOperator(PhysicalPlan &physical_plan, PhysicalOperatorType type,
                                         vector<LogicalType> types, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality) {
}
PhysicalTeeOperator::~PhysicalTeeOperator() {

}

}; // namespace duckdb
