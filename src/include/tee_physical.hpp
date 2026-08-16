#pragma once

#include "tee_extension.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"

namespace duckdb {

class PhysicalTee : public PhysicalOperator {
public:
	PhysicalTee(PhysicalPlan &physical_plan, vector<LogicalType> types, vector<string> names,
	            idx_t estimated_cardinality, idx_t projected_input_count, named_parameter_map_t tee_named_parameters);

	vector<string> names_output;
	idx_t projected_input_count;
	TeeOptions options;
	vector<LogicalType> tee_types;

	string GetName() const override {
		return "tee";
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &global_state, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return !options.path_flag;
	}

	bool RequiresOperatorFinalize() const override {
		return true;
	}

	OperatorFinalResultType OperatorFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                                         OperatorFinalizeInput &input) const override;

	// find out whether we have a recursive CTE
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override {
		is_recursive_cte = meta_pipeline.HasRecursiveCTE();
		// pass back to base class
		PhysicalOperator::BuildPipelines(current, meta_pipeline);
	}

private:
	bool is_recursive_cte = false;

	string StateKey() const {
		return to_string(reinterpret_cast<uintptr_t>(this));
	}
};

} // namespace duckdb