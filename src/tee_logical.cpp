#include "tee_extension.hpp"
#include "tee_logical.hpp"
#include "tee_physical.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/execution/operator/projection/physical_tableinout_function.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"

namespace duckdb {

// Dummy bind data
struct TeeInOutBindData : public FunctionData {
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<TeeInOutBindData>();
	}
	bool Equals(const FunctionData &other) const override {
		return true;
	}
};

// Dummy in/out function
static OperatorResultType TeeDummyInOut(ExecutionContext &context, TableFunctionInput &data, DataChunk &input,
                                        DataChunk &output) {
	std::cout << "TeeDummyInOut InOut\n";
	output.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

string LogicalTeeOperator::GetExtensionName() const {
	return "tee";
}

void LogicalTeeOperator::ResolveTypes() {
	if (children.empty()) {
		return;
	}
	types = children[0]->types;
}

bool LogicalTeeOperator::RequireOptimizer() const {
	return false;
}

idx_t LogicalTeeOperator::GetRootIndex() {
	if (children.empty()) {
		return bind_index;
	}
	return children[0]->GetRootIndex();
}

vector<ColumnBinding> LogicalTeeOperator::GetColumnBindings() {
	if (children.empty()) {
		return LogicalOperator::GenerateColumnBindings(bind_index, types.size());
	}
	return children[0]->GetColumnBindings();
}

PhysicalOperator &LogicalTeeOperator::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	auto &child = planner.CreatePlan(*children[0]);
	std::cerr << "LogicalTeeOperator CreatePlan is called \n";
	std::cerr << " child.types.size() = " << child.types.size() << "\n";
	std::cerr << " child.GetTypes().size() = " << child.GetTypes().size() << "\n";

	// Build a dummy TableFunction
	TableFunction logical_table_function;
	logical_table_function.name = "tee";
	logical_table_function.in_out_function = TeeDummyInOut;
	logical_table_function.in_out_function_final = nullptr;

	auto bind_data = make_uniq<TeeInOutBindData>();

	vector<ColumnIndex> column_ids;
	column_ids.reserve(child.types.size());
	for (idx_t i = 0; i < child.types.size(); i++) {
		column_ids.emplace_back(i);
	}

	vector<column_t> projected_input;

	auto &tee_inout = planner.Make<PhysicalTableInOutFunction>(child.types, logical_table_function,
	                                                           std::move(bind_data), std::move(column_ids),
	                                                           child.estimated_cardinality, std::move(projected_input));

	tee_inout.children.push_back(child);
	return tee_inout;
}
} // namespace duckdb
