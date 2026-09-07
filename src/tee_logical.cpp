#include "include/tee_logical.hpp"
#include "include/tee_physical.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

LogicalTee::LogicalTee(TableIndex table_idx_p, named_parameter_map_t tee_named_parameters_p)
    : table_index(table_idx_p), tee_named_parameters(std::move(tee_named_parameters_p)) {
}

void LogicalTee::ResolveTypes() {
	types = children[0]->types;
}

vector<ColumnBinding> LogicalTee::PushdownDependentJoin(FlattenDependentJoins &flattener,
                                                        unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
                                                        vector<ColumnBinding> column_bindings,
                                                        BindingReplacementGraph &replacement_graph) {
	D_ASSERT(plan->children.size() == 1);

	// hand the correlated columns to the projection below us
	auto result = PushDownDependentJoinChild(flattener, plan, propagate_null_values, std::move(column_bindings),
	                                         replacement_graph, 0);
	ResolveOperatorTypes();
	return result;
}

PhysicalOperator &LogicalTee::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	D_ASSERT(children.size() == 1);

	vector<string> names;
	names.reserve(types.size());
	if (children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		// read the column names from the projection below us
		for (const auto &expr : children[0]->expressions) {
			names.push_back(expr->GetAlias().GetIdentifierName());
		}
	} else {
		// if our projection was replaced somehow, we still have to provide column names
		for (idx_t i = 0; i < types.size(); i++) {
			names.push_back("col" + to_string(i));
		}
	}
	D_ASSERT(names.size() == types.size());

	auto &child = planner.CreatePlan(*children[0]);

	auto &physical_tee = planner.Make<PhysicalTee>(types, names, estimated_cardinality, tee_named_parameters);
	physical_tee.children.push_back(child);

	return physical_tee;
}

} // namespace duckdb