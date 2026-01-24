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
	LogicalTeeOperator() : LogicalExtensionOperator() {
	}

	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_GET;
	vector<string> names;
	idx_t bind_index = DConstants::INVALID_INDEX;

	string GetExtensionName() const override {
		return "tee";
	}

	void ResolveTypes() override {
		if (children.empty()) {
			return;
		}
		types = children[0]->types;
	}

	bool RequireOptimizer() const override {
		// this is true on default - rn it's for testing
		return true;
	}

	idx_t GetRootIndex() override {
		if (children.empty()) {
			return bind_index;
		}
		return children[0]->GetRootIndex();
	}

	vector<ColumnBinding> GetColumnBindings() override {
		if (children.empty()) {
			return LogicalOperator::GenerateColumnBindings(bind_index, types.size());
		}
		return children[0]->GetColumnBindings();
	}

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		auto &child = planner.CreatePlan(*children[0]);
		auto &tee = planner.Make<PhysicalTeeOperator>(child.types, names, child.estimated_cardinality);

		tee.children.push_back(child);

		return tee;
	}
};
} // namespace duckdb