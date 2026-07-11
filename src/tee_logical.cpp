#include "include/tee_logical.hpp"
#include "include/tee_physical.hpp"

namespace duckdb {

LogicalTee::LogicalTee(TableIndex table_idx_p, vector<LogicalType> types_output_p, vector<string> names_output_p,
                       named_parameter_map_t tee_named_parameters_p)
    : table_index(table_idx_p), types_output(std::move(types_output_p)), names_output(std::move(names_output_p)),
      tee_named_parameters(std::move(tee_named_parameters_p)) {
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
	types = children[0]->types;
}

vector<ColumnBinding> LogicalTee::PushdownDependentJoin(FlattenDependentJoins &flattener,
                                                        unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
                                                        vector<ColumnBinding> column_bindings) {
	Printer::Print("Debug: LogicalTee::PushdownDependentJoin was called");
	D_ASSERT(plan->children.size() == 1);

	// push the dependent join into our only child
	column_bindings = flattener.PushDownExtensionChild(plan, propagate_null_values, std::move(column_bindings), 0);

	// pass the correlated columns through
	auto child_bindings = children[0]->GetColumnBindings();
	for (auto &binding : column_bindings) {
		for (idx_t i = 0; i < child_bindings.size(); i++) {
			if (child_bindings[i] == binding) {
				projected_input.push_back(i);
				break;
			}
		}
	}
	ResolveOperatorTypes();
	return column_bindings;
}

PhysicalOperator &LogicalTee::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	D_ASSERT(children.size() == 1);

	auto &child = planner.CreatePlan(*children[0]);

	auto &physical_tee = planner.Make<PhysicalTee>(types, names_output, estimated_cardinality,
	                                               static_cast<idx_t>(projected_input.size()), tee_named_parameters);
	physical_tee.children.push_back(child);

	return physical_tee;
}

} // namespace duckdb