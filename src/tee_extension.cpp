#include "tee_extension.hpp"
#include "tee_parser.hpp"
#include <regex>
#include "duckdb/common/csv_writer.hpp"

#include <duckdb/main/connection_manager.hpp>

namespace duckdb {

// this function is called once per chunk
static OperatorResultType TeeTableFun(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                      DataChunk &output) {
	// access to the global state object
	auto &global_state = data_p.global_state->Cast<TeeGlobalState>();

	// append the current chunk to the global buffer
	global_state.buffered.Append(input);

	// pass through the input chunk unchanged to the output
	output.Reference(input);

	return OperatorResultType::NEED_MORE_INPUT;
}

static void TeeCSVWriter(const ExecutionContext &context, TableFunctionInput &data_p, DataChunk &output, string &path) {

	auto &global_state = data_p.global_state->Cast<TeeGlobalState>();

	// write always in my local dir
	path = "csv_files_testing/" + path;
	std::cout << "Write to: " << path << "\n";

	FileSystem &fs = FileSystem::GetFileSystem(context.client);

	// prepare options
	CSVReaderOptions options;
	options.name_list = global_state.names;
	// set own names
	options.columns_set = true;
	options.force_quote.resize(global_state.names.size(), false);

	// initialize state
	CSVWriterState write_state(context.client,
	                           4096ULL * 8ULL); // in csv_writer.hpp they used: idx_t flush_size = 4096ULL * 8ULL;
	// initialize writer
	CSVWriter writer(options, fs, path, FileCompressionType::UNCOMPRESSED, false);

	// force writing header and prefix
	writer.Initialize(true);

	// scan/initialize ColumnDataCollection with our buffered data
	// this is the place where our column data lives
	ColumnDataScanState scan_state;
	global_state.buffered.InitializeScan(scan_state);

	// initialize chunk
	DataChunk chunk;
	global_state.buffered.InitializeScanChunk(scan_state, chunk);

	// as long as we have data write it
	while (global_state.buffered.Scan(scan_state, chunk)) {
		// WriteChunk writes only VARCHAR columns
		// limitation for now
		writer.WriteChunk(chunk, write_state);
		chunk.Reset();
	}

	writer.Flush(write_state);
	writer.Close();
}

static void TeeTableWriter(ExecutionContext &context, TableFunctionInput &data_p, DataChunk & output, string &table_name) {
	const auto &db = context.client.db;
	const auto &con_list = db->GetConnectionManager().GetConnectionList();

	if (con_list.size() != 1) {
		return;
	}
}

static OperatorFinalizeResultType TeeFinalize(ExecutionContext &context, TableFunctionInput &data_p,
                                              DataChunk &output) {

	auto &global_state = data_p.global_state->Cast<TeeGlobalState>();
	auto &parameter_map = data_p.bind_data->Cast<TeeBindData>().tee_named_parameters;

	// terminal output is the default behavior
	bool terminal_flag = true;

	auto const it_terminal = parameter_map.find("terminal");
	auto const it_symbol = parameter_map.find("symbol");
	auto const it_csv_path = parameter_map.find("path");
	auto const it_table = parameter_map.find("table_name");

	if (it_terminal != parameter_map.end()) {
		terminal_flag = parameter_map.at("terminal").GetValue<bool>();
	}

	if (it_csv_path != parameter_map.end()) {
		string path = parameter_map.at("path").GetValue<string>();
		TeeCSVWriter(context, data_p, output, path);
	}

	if (it_table != parameter_map.end()) {
		string table_name = " ";
		TeeTableWriter(context, data_p, output, table_name);
	}

	// prints only once
	if (!global_state.printed && terminal_flag) {
		if (it_symbol != parameter_map.end()) {
			string symbol = parameter_map.at("symbol").GetValue<string>();
			std::cout << "Tee Operator, Query: " << symbol << "\n";
		} else {
			std::cout << "Tee Operator: \n";
		}

		auto renderer = BoxRenderer();

		renderer.Print(context.client, global_state.names, global_state.buffered);

		global_state.printed = true;
	}
	return OperatorFinalizeResultType::FINISHED;
}

// this function is called once at the start of execution to create the global state
static unique_ptr<GlobalTableFunctionState> TeeInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<TeeBindData>();
	return make_uniq<TeeGlobalState>(context, bind_data.types, bind_data.names);
}

// runs when a query runs, decides the schema of the table function output
static unique_ptr<FunctionData> TeeBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names, named_parameter_map_t named_parameters) {
	names = input.input_table_names;
	return_types = input.input_table_types;

	// returns a bind data object
	return make_uniq<TeeBindData>(names, return_types, input.named_parameters);
}

// called when the extension is loaded
// registers the tee table function and the parser extension
static void LoadInternal(ExtensionLoader &loader) {

	TableFunction tee_function("tee", {LogicalType::TABLE}, nullptr, reinterpret_cast<table_function_bind_t>(TeeBind));
	tee_function.init_global = TeeInitGlobal;
	tee_function.named_parameters["path"] = LogicalType::VARCHAR;
	tee_function.named_parameters["symbol"] = LogicalType::VARCHAR;
	tee_function.named_parameters["terminal"] = LogicalType::BOOLEAN;
	tee_function.named_parameters["table_name"] = LogicalType::VARCHAR;
	tee_function.in_out_function = TeeTableFun;
	tee_function.in_out_function_final = TeeFinalize;
	loader.RegisterFunction(tee_function);

	ParserExtension tee_parser {};
	tee_parser.parser_override = TeeParserExtension::ParserOverrideFunction;
	tee_parser.parse_function = TeeParserExtension::ParseFunction;
	tee_parser.plan_function = TeeParserExtension::PlanFunction;

	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.options.allow_parser_override_extension = "fallback";
	config.parser_extensions.push_back(tee_parser);
}

void TeeExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(tee, loader) {
	duckdb::LoadInternal(loader);
}
}