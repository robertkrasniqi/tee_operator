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

// probably unnecessary; maybe delete
struct TeeGlobalOperatorState : public GlobalOperatorState {
	TeeGlobalOperatorState(ClientContext &context, vector<LogicalType> types_p, vector<string> names_p)
	    : buffered(context, types_p), types(std::move(types_p)), names(std::move(names_p)) {
	}
	mutex lock;
	ColumnDataCollection buffered;
	vector<LogicalType> types;
	vector<string> names;
};

class PhysicalTeeOperator : public PhysicalOperator {
public:
	static constexpr PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;
	PhysicalTeeOperator(PhysicalPlan &physical_plan, vector<LogicalType> types, vector<string> names,
	                    idx_t estimated_cardinality);
	~PhysicalTeeOperator() override;

	vector<string> names;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;

	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}

	bool IsSource() const override {
		return true;
	}

	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

	vector<const_reference<PhysicalOperator>> GetSources() const override;

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override {
		return make_uniq<OperatorState>();
	}

	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override {
		return make_uniq<TeeGlobalOperatorState>(context, types, names);
	}

	/*
	// No need
	bool RequiresFinalExecute() const override {
	    return false;
	}

	// No need
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &output,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;
	*/
	string GetName() const override;
};
} // namespace duckdb
