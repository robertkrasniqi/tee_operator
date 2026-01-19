#include "tee_operator.hpp"
#include "tee_extension.hpp"

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

OperatorResultType PhysicalTeeOperator::Execute(ExecutionContext &context, DataChunk &input, DataChunk &output,
                                                GlobalOperatorState &gstate, OperatorState &state) const {
	auto &tee_state = gstate.Cast<TeeGlobalOperatorState>();

	tee_state.lock.lock();

	tee_state.buffered.Append(input);
	output.Reference(input);
	std::cout << "this is a execute \n";

	tee_state.lock.unlock();
	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorFinalizeResultType PhysicalTeeOperator::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                             GlobalOperatorState &gstate, OperatorState &state) const {
	auto &tee_state = gstate.Cast<TeeGlobalOperatorState>();

	std::cout << "Final Execute \n";
	BoxRendererConfig config;
	BoxRenderer renderer(config);

	Printer::RawPrint(OutputStream::STREAM_STDOUT, "Tee Operator Operator:\n");
	renderer.Print(context.client, names, tee_state.buffered);

	return OperatorFinalizeResultType::FINISHED;
};
string PhysicalTeeOperator::GetName() const {
	return "tee";
}

} // namespace duckdb