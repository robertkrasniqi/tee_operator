#define DUCKDB_EXTENSION_MAIN

#include "tee_extension.hpp"

namespace duckdb {

static OperatorResultType TeeTableFun(ExecutionContext &context, TableFunctionInput &data_p,
                                      DataChunk &input, DataChunk &output) {
	// TODO: currently we print the data for every chunk processed.
	// We want to change this to only print once per table function call.
	// This will require some state management to ensure we only print once at the end.
	std::cout << "Tee Operator" << std::endl;

	// Cast the bind data to TeeBindData
	auto bind_data = data_p.bind_data->Cast<TeeBindData>();
	auto renderer = BoxRenderer();

	// Construct a new ColumnDataCollection that the renderer can scan
	ColumnDataCollection scan(context.client, input.GetTypes());
	scan.Append(input);

	// Use the renderer to print the data
	renderer.Print(context.client, bind_data.names, scan);

	// Set a reference to the unchanged input chunk
	output.Reference(input);

	return OperatorResultType::NEED_MORE_INPUT;
}

static unique_ptr<FunctionData> TeeBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {

	// Get the input table names from the bind input
	names = input.input_table_names;

	// We're only doing side effects here, so there's no need to modify the return types
	return_types = input.input_table_types;

	return make_uniq<TeeBindData>(names);
}

static void LoadInternal(DatabaseInstance &instance) {
	// Create a new table function
	auto tee_function = TableFunction("tee", {LogicalType::TABLE}, nullptr, TeeBind);

	// Set TeeTableFun as the in_out_function for the table function
	tee_function.in_out_function = TeeTableFun;

	// Register the table function within the database instance
	ExtensionUtil::RegisterFunction(instance, tee_function);
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
