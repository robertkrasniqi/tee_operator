#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "tee_parser.hpp"
#include "tee_extension.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator_extension.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/common/mutex.hpp"
#include <duckdb/main/connection.hpp>
#include <duckdb/planner/planner.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>

namespace duckdb {
class PhysicalTeeOperator : public PhysicalOperator {
public:
	static constexpr PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	PhysicalTeeOperator(PhysicalPlan &physical_plan, PhysicalOperatorType type, vector<LogicalType> types,
	                    idx_t estimated_cardinality);
	~PhysicalTeeOperator() override;

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override {
		return make_uniq<OperatorState>();
	}

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &output,
	                           GlobalOperatorState &gstate, OperatorState &state) const override {
		output.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}

public:
	string GetName() const override {
		return "tee";
	}
};

class LogicalTeeOperator : public LogicalExtensionOperator {
public:
	LogicalTeeOperator() : LogicalExtensionOperator() {
	}

	string GetExtensionName() const override {
		return "tee";
	}

	void ResolveTypes() override {
		types = children[0]->types;
	}

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		auto &child = planner.CreatePlan(*children[0]);

		auto &tee = planner.Make<PhysicalTeeOperator>(
			PhysicalOperatorType::EXTENSION,
			child.types,
			child.estimated_cardinality);
		tee.children.push_back(child);
		return tee;
	}
};

BoundStatement TeeBindParser(ClientContext &context, Binder &binder, OperatorExtensionInfo *info,
                             SQLStatement &statement);

struct TeeOperatorExtension : public OperatorExtension {
	TeeOperatorExtension() {
		Bind = TeeBindParser;
	}

	string GetName() override {
		return "tee";
	}

	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &) override {
		return make_uniq<LogicalTeeOperator>();
	}
};
} // namespace duckdb
