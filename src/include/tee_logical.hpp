#pragma once

#include "tee_parser.hpp"
#include "tee_extension.hpp"
#include "tee_physical.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/binder.hpp"
#include <duckdb/execution/column_binding_resolver.hpp>
#include <duckdb/main/connection.hpp>
#include "duckdb/main/config.hpp"

namespace duckdb {

class LogicalTeeOperator : public LogicalExtensionOperator {
public:
	vector<string> names;
	idx_t bind_index = DConstants::INVALID_INDEX;

	string GetExtensionName() const override;

	void ResolveTypes() override;

	bool RequireOptimizer() const override;

	idx_t GetRootIndex() override;

	vector<ColumnBinding> GetColumnBindings() override;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;
};
} // namespace duckdb