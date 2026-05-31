#include "include/tee_logical.hpp"
#include "include/tee_physical.hpp"

namespace duckdb {

LogicalTee::LogicalTee(TableIndex table_idx_p, vector<LogicalType> types_output_p, vector<string> names_output_p)
    : table_index(table_idx_p), types_output(std::move(types_output_p)), names_output(std::move(names_output_p)) {
}

vector<ColumnBinding> LogicalTee::GetColumnBindings() {
	vector<ColumnBinding> result;
	result.reserve(types_output.size());
	for (idx_t i = 0; i < types_output.size(); i++) {
		result.emplace_back(table_index, ProjectionIndex(i));
	}
	return result;
}

void LogicalTee::ResolveTypes() {
	types = types_output;
}

PhysicalOperator &LogicalTee::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	D_ASSERT(children.size() == 1);

	auto &child = planner.CreatePlan(*children[0]);

	auto &physical_tee = planner.Make<PhysicalTee>(types_output, estimated_cardinality);
	physical_tee.children.push_back(child);

	return physical_tee;
}

} // namespace duckdb