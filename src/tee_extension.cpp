#define DUCKDB_EXTENSION_MAIN

#include "tee_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

inline void TeeScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Tee " + name.GetString() + " 🐥");
	});
}



static void LoadInternal(DatabaseInstance &instance) {
	// Register a scalar function
	auto tee_scalar_function = ScalarFunction("tee", {LogicalType::VARCHAR}, LogicalType::VARCHAR, TeeScalarFun);
	ExtensionUtil::RegisterFunction(instance, tee_scalar_function);
}

void TeeExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string TeeExtension::Name() {
	return "tee";
}

std::string TeeExtension::Version() const {
#ifdef EXT_VERSION_TEE
	return EXT_VERSION_TEE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void tee_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::TeeExtension>();
}

DUCKDB_EXTENSION_API const char *tee_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
