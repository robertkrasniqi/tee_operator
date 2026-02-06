#include "tee_extension.hpp"
#include "tee_logical.hpp"
#include "tee_physical.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {
PhysicalTeeOperator::PhysicalTeeOperator(PhysicalPlan &physical_plan, vector<LogicalType> types, vector<string> names_p,
                                         idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      names(std::move(names_p)) {
}

PhysicalTeeOperator::~PhysicalTeeOperator() {
}

string PhysicalTeeOperator::GetName() const {
	return "tee";
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

class TeeSinkState : public GlobalSinkState {
public:
	explicit TeeSinkState(ClientContext &context, const PhysicalTeeOperator &op)
	    : buffered_sink(context, op.GetTypes()) {
	}
	mutex lock;
	ColumnDataCollection buffered_sink;
	ColumnDataScanState scan_state;
	bool initialized = false;
};

unique_ptr<GlobalSinkState> PhysicalTeeOperator::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<TeeSinkState>(context, *this);
}

SinkResultType PhysicalTeeOperator::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &state = input.global_state.Cast<TeeSinkState>();

	state.lock.lock();
	{
		state.buffered_sink.Append(chunk);
	}
	state.lock.unlock();
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalTeeOperator::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	auto &state = input.global_state.Cast<TeeSinkState>();

	BoxRendererConfig config;
	BoxRenderer renderer(config);

	Printer::RawPrint(OutputStream::STREAM_STDOUT, "Tee Operator Operator: \n");
	renderer.Print(context, names, state.buffered_sink);

	return SinkFinalizeType::READY;
}
//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

SourceResultType PhysicalTeeOperator::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSourceInput &input) const {
	auto &state = sink_state->Cast<TeeSinkState>();

	if (!state.initialized) {
		state.buffered_sink.InitializeScan(state.scan_state);
		state.initialized = true;
	}

	state.buffered_sink.Scan(state.scan_state, chunk);

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

vector<const_reference<PhysicalOperator>> PhysicalTeeOperator::GetSources() const {
	return {*this};
}

void PhysicalTeeOperator::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	sink_state.reset();

	auto &state = meta_pipeline.GetState();
	state.SetPipelineSource(current, *this);

	auto &child_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	child_pipeline.Build(children[0]);
}

} // namespace duckdb