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

class PhysicalTeeOperator {};

} // namespace duckdb
