#include "include/tee_logical.hpp"
#include "include/tee_physical.hpp"

namespace duckdb {

LogicalTee::LogicalTee(TableIndex table_idx_p, vector<LogicalType> types_output_p, vector<string> names_output_p)
    : table_index(table_idx_p), types_output(std::move(types_output_p)), names_output(std::move(names_output_p)) {
}

vector<ColumnBinding> LogicalTee::GetColumnBindings() {
	// column_bindings should contain our columns + the projected children columns
	vector<ColumnBinding> column_bindings;
	column_bindings.reserve(types_output.size() + projected_input.size());
	for (idx_t i = 0; i < types_output.size(); i++) {
		column_bindings.emplace_back(table_index, ProjectionIndex(i));
	}
	if (!projected_input.empty()) {
		D_ASSERT(children.size() == 1);
		auto child_column_bindings = children[0]->GetColumnBindings();
		for (const auto col : projected_input) {
			column_bindings.emplace_back(child_column_bindings[col]);
		}
	}
	return column_bindings;
}

void LogicalTee::ResolveTypes() {
	types = types_output;
	if (!projected_input.empty()) {
		D_ASSERT(children.size() == 1);
		for (auto col : projected_input) {
			types.push_back(children[0]->types[col]);
		}
	}
}

PhysicalOperator &LogicalTee::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	D_ASSERT(children.size() == 1);

	auto &child = planner.CreatePlan(*children[0]);

	auto &physical_tee = planner.Make<PhysicalTee>(types, names_output, estimated_cardinality,
	                                               static_cast<idx_t>(projected_input.size()));
	physical_tee.children.push_back(child);

	return physical_tee;
}

} // namespace duckdb