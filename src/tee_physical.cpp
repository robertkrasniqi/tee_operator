#include "include/tee_physical.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/box_renderer_context.hpp"
#include "duckdb/common/column_data_collection_render_interface.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/execution/physical_operator_states.hpp"

namespace duckdb {

PhysicalTee::PhysicalTee(PhysicalPlan &physical_plan, vector<LogicalType> types_p, vector<string> names_p,
                         idx_t estimated_cardinality, idx_t projected_input_count_p,
                         named_parameter_map_t tee_named_parameters_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      names_output(std::move(names_p)), projected_input_count(projected_input_count_p),
      tee_named_parameters(std::move(tee_named_parameters_p)) {
}

unique_ptr<GlobalOperatorState> PhysicalTee::GetGlobalOperatorState(ClientContext &context) const {
	idx_t original_col_count = types.size() - projected_input_count;
	return make_uniq<TeeGlobalState>(context, types, names_output, original_col_count, tee_named_parameters);
}

OperatorResultType PhysicalTee::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                        GlobalOperatorState &global_state, OperatorState &state) const {
	auto &tee_state = global_state.Cast<TeeGlobalState>();
	idx_t original_col_count = tee_state.all_col_count;

	// no correlation -> normal buffering
	if (projected_input_count == 0) {
		{
			lock_guard<mutex> guard(tee_state.lock);
			tee_state.buffered.Append(input);
		}
		chunk.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}
	// correlated case:
	// we only want to display (and buffer) the original cols, but pass the full input (which includes projected_input)
	{
		lock_guard<mutex> guard(tee_state.lock);
		// build a chunk that references only the first original columns
		DataChunk original_chunk;
		const vector<LogicalType> original_types(types.begin(), types.begin() + original_col_count);
		original_chunk.InitializeEmpty(original_types);
		for (idx_t i = 0; i < original_col_count; i++) {
			original_chunk.data[i].Reference(input.data[i]);
		}
		original_chunk.SetCardinality(input.size());
		tee_state.buffered.Append(original_chunk);
	}
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorFinalResultType PhysicalTee::OperatorFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                      OperatorFinalizeInput &input) const {
	auto &tee_state = input.global_state.Cast<TeeGlobalState>();

	ColumnDataCollectionWrapper render_buffer(tee_state.buffered);
	ClientBoxRendererContext render_context(context);

	Printer::RawPrint(OutputStream::STREAM_STDOUT, "Tee Operator: \n");

	BoxRendererConfig config;
	BoxRenderer renderer(config);
	renderer.Print(render_context, tee_state.names, render_buffer);

	return OperatorFinalResultType::FINISHED;
}
} // namespace duckdb
