#include "include/tee_physical.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/box_renderer_context.hpp"
#include "duckdb/common/column_data_collection_render_interface.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/execution/physical_operator_states.hpp"

namespace duckdb {

PhysicalTee::PhysicalTee(PhysicalPlan &physical_plan, vector<LogicalType> types_p, vector<string> names_p,
                         idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types_p, estimated_cardinality),
      names_output(std::move(names_p)) {
}

unique_ptr<GlobalOperatorState> PhysicalTee::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<TeeGlobalState>(context, types, names_output);
}

OperatorResultType PhysicalTee::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                        GlobalOperatorState &global_state, OperatorState &state) const {
	{
		auto &tee_state = global_state.Cast<TeeGlobalState>();
		lock_guard<mutex> guard(tee_state.lock);
		tee_state.buffered.Append(input);
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
