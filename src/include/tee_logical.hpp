#pragma once

#include "tee_parser.hpp"
#include "tee_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "tee_physical.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/binder.hpp"
#include <duckdb/execution/column_binding_resolver.hpp>
#include <duckdb/main/connection.hpp>
#include "duckdb/main/config.hpp"
#include <duckdb/execution/operator/projection/physical_tableinout_function.hpp>

namespace duckdb {

class LogicalTeeOperator : public LogicalGet {
public:
	LogicalTeeOperator(idx_t table_index, TableFunction function, unique_ptr<FunctionData> bind_data,
	                   vector<LogicalType> returned_types, vector<string> returned_names)
	    : LogicalGet(table_index, std::move(function), std::move(bind_data), std::move(returned_types),
	                 std::move(returned_names)) {
	}

	string GetExtensionName() const {
		return "tee";
	}
};

} // namespace duckdb