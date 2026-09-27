#pragma once

#include "tee_extension.hpp"
#include "duckdb/common/csv_writer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"

namespace duckdb {

class TeeLocalState;

//! Holds the result ColumnDataCollection
//! Is responsible for combining local states
//! Initializes the CSV and table writer
//! Keeps track of the current iteration step
class TeeGlobalState : public GlobalOperatorState {
public:
	TeeGlobalState(ClientContext &context, const TeeOptions &options, const vector<string> &names,
	               const vector<LogicalType> &types, bool recursive_iteration);
	~TeeGlobalState() override;

	void WriteChunkToTable(ClientContext &context, DataChunk &chunk, TeeLocalState &l_state);
	void WriteChunkToCSV(ClientContext &context, DataChunk &chunk, TeeLocalState &l_state) const;

	void AppendLocalToGlobalBuffer(ColumnDataCollection &local_buffer) {
		lock_guard<mutex> guard(buffer_lock);
		buffered->Combine(local_buffer);
	}

	void NextIteration() {
		++iteration_step;
	}

	idx_t CurrentIteration() const {
		return iteration_step;
	}

	unique_ptr<ColumnDataCollection> buffered;

private:
	bool is_recursive_cte;
	atomic<idx_t> iteration_step {0};
	mutex buffer_lock;
	unique_ptr<CSVWriter> csv_writer;
	unique_ptr<Connection> con;
	unique_ptr<Appender> appender;
	mutex appender_lock;

	void TeeInitializeCSVWriter(ClientContext &context, const TeeOptions &options, const vector<string> &names);
	void TeeInitializeTableWriter(ClientContext &context, const TeeOptions &options, const vector<string> &names,
	                              const vector<LogicalType> &types);
};

//! State of a single thread
//! Holds a local ColumnDataCollection which gets appended to the result ColumnDataCollection when the thread is done
class TeeLocalState : public OperatorState {
public:
	TeeLocalState(ClientContext &context, const TeeOptions &options, const vector<LogicalType> &tee_types,
	              bool is_recursive_cte);

	unique_ptr<ColumnDataCollection> local_buffer;
	ColumnDataAppendState local_append_state;
	unique_ptr<CSVWriterState> local_csv_state;
	DataChunk varchar_chunk_csv;
	// only used inside recursive CTEs, carries the iteration index in column 0
	DataChunk chunk_with_iteration_column;

	// Appends the local buffer to the global buffer
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		if (local_buffer) {
			op.op_state->Cast<TeeGlobalState>().AppendLocalToGlobalBuffer(*local_buffer);
			local_buffer->InitializeAppend(local_append_state);
		}
	}

	bool SupportsReuse() const override {
		return true;
	}
};

class PhysicalTee : public PhysicalOperator {
public:
	PhysicalTee(PhysicalPlan &physical_plan, vector<LogicalType> types, vector<string> names,
	            idx_t estimated_cardinality, const named_parameter_map_t &tee_named_parameters);

	vector<string> names_output;
	TeeOptions tee_options;

	string GetName() const override {
		return "tee";
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;

	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override {
		return make_uniq<TeeGlobalState>(context, tee_options, names_output, types, is_recursive_cte);
	}

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override {
		return make_uniq<TeeLocalState>(context.client, tee_options, types, is_recursive_cte);
	}

	// Reset needed to enable the counting of steps and writing into CSVs with recursive CTEs
	bool ResetGlobalOperatorState(ClientContext &context, GlobalOperatorState &state) const override {
		return true;
	}

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &global_state, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

	bool RequiresOperatorFinalize() const override {
		return true;
	}

	// find out whether we have a recursive CTE
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override {
		is_recursive_cte = meta_pipeline.HasRecursiveCTE();
		// pass back to base class
		PhysicalOperator::BuildPipelines(current, meta_pipeline);
	}

	OperatorFinalResultType OperatorFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                                         OperatorFinalizeInput &input) const override;

private:
	bool is_recursive_cte = false;
};

} // namespace duckdb