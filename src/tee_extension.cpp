#include "tee_extension.hpp"
#include "tee_parser.hpp"
#include "duckdb/common/csv_writer.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/connection_manager.hpp"

namespace duckdb {

// this function is called once per chunk
static OperatorResultType TeeTableFun(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                      DataChunk &output) {
	// access to the global state object
	auto &global_state = data_p.global_state->Cast<TeeGlobalState>();

	// lock the global_space to prevent race conditions
	// and append the current chunk to the global buffer
	global_state.lock.lock();
	global_state.buffered.Append(input);
	output.Reference(input);
	global_state.lock.unlock();

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

	// as long as we have data, write it
	while (global_state.buffered.Scan(scan_state, chunk)) {
		// CSVWriter expects varchar columns. For that, we cast our current chunk into a new chunk
		// which has only varchar columns.
		DataChunk varchar_chunk;
		vector<LogicalType> varchar_vector;
		varchar_vector.reserve(chunk.ColumnCount());
		for (idx_t i = 0; i < chunk.ColumnCount(); ++i) {
			varchar_vector.emplace_back(LogicalType::VARCHAR);
		}
		// initialize chunk with the same client context
		varchar_chunk.Initialize(context.client, varchar_vector);
		idx_t rows = chunk.size();
		for (idx_t col = 0; col < chunk.ColumnCount(); ++col) {
			VectorOperations::DefaultCast(chunk.data[col], varchar_chunk.data[col], rows, false);
		}
		// Tell the chunk how many rows it has. If we don't, we write 0 rows.
		varchar_chunk.SetCardinality(rows);

		writer.WriteChunk(varchar_chunk, write_state);
		varchar_chunk.Reset();
		chunk.Reset();
	}

	writer.Flush(write_state);
	writer.Close();
}

static void TeeTableWriter(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &output,
                           string &table_name) {
	auto &bind_data = data_p.bind_data->Cast<TeeBindData>();
	auto &global_state = data_p.global_state->Cast<TeeGlobalState>();

	auto &db = context.client.db->GetDatabase(context.client);
	Connection con(db);

	vector<string> names = bind_data.names;
	vector<LogicalType> types = bind_data.types;

	// copy the name and type schema of the current subquery for the new table
	string name_types = "";
	for (idx_t i = 0; i < names.size(); i++) {
		name_types += " " + names[i] + " " + types[i].ToString();
		if (i + 1 < names.size()) {
			name_types += ", ";
		};
	}

	string sql_statement = "CREATE OR REPLACE TABLE " + table_name + "(" + name_types + ")";

	con.Query(sql_statement);

	// create an appender on the existing context/database
	// is responsible for writing the actual rows in the table
	Appender appender(con, table_name);

	ColumnDataScanState scan_state;
	global_state.buffered.InitializeScan(scan_state);

	DataChunk chunk;
	global_state.buffered.InitializeScanChunk(scan_state, chunk);

	while (global_state.buffered.Scan(scan_state, chunk)) {
		idx_t rows = chunk.size();
		for (idx_t cur_row = 0; cur_row < rows; cur_row++) {
			appender.BeginRow();
			for (idx_t cur_col = 0; cur_col < chunk.ColumnCount(); cur_col++) {
				// read value from chunk
				Value value = chunk.GetValue(cur_col, cur_row);
				// write value to appender
				appender.Append(value);
			}
			appender.EndRow();
		}
		chunk.Reset();
	}
	// write everything from the buffer before closing
	appender.Close();
}

static void TeeWriteResult(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &output) {
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
		string table_name = parameter_map.at("table_name").GetValue<string>();
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
		BoxRendererConfig config;
		// config.max_rows = 999999;
		// config.render_mode = RenderMode::COLUMNS;
		BoxRenderer renderer(config);
		BoxRenderer s {};
		StringResultRenderer base;
		s.Render(context.client, global_state.names, global_state.buffered, base);
		std::cout << s.ToString(context.client, global_state.names, global_state.buffered) << "\n";
		// renderer.Print(context.client, global_state.names, global_state.buffered);
		global_state.printed = true;
	}
}

// gets called once per thread when the thread is done
static OperatorFinalizeResultType TeeFinalize(ExecutionContext &context, TableFunctionInput &data_p,
                                              DataChunk &output) {
	auto &global_state = data_p.global_state->Cast<TeeGlobalState>();
	atomic<int> &active_threads = global_state.active_threads;

	// decrement active_threads until we have 0 left
	// 0 means we accessed with the last thread and
	// the last thread can do the real writing work sss
	--active_threads;

	if (active_threads == 0) {
		TeeWriteResult(context, data_p, output);
	}
	return OperatorFinalizeResultType::FINISHED;
}

// this function is called once at the start of execution to create the global state
static unique_ptr<GlobalTableFunctionState> TeeInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<TeeBindData>();
	return make_uniq<TeeGlobalState>(context, bind_data.types, bind_data.names);
}

// gets called once per thread
static unique_ptr<LocalTableFunctionState> TeeInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                        GlobalTableFunctionState *global_state_p) {
	auto &global_state = global_state_p->Cast<TeeGlobalState>();
	// For every thread, increment by 1. Need that to decrement later to
	// the correct number of threads really used.
	++global_state.active_threads;
	return make_uniq<TeeLocalState>();
}

static unique_ptr<FunctionData> TeeBind(ClientContext &context, const TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
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
	tee_function.init_local = TeeInitLocal;
	tee_function.in_out_function = TeeTableFun;
	tee_function.in_out_function_final = TeeFinalize;
	tee_function.named_parameters["path"] = LogicalType::VARCHAR;
	tee_function.named_parameters["symbol"] = LogicalType::VARCHAR;
	tee_function.named_parameters["terminal"] = LogicalType::BOOLEAN;
	tee_function.named_parameters["table_name"] = LogicalType::VARCHAR;
	loader.RegisterFunction(tee_function);

	ParserExtension tee_parser {};
	tee_parser.parser_override = TeeParserExtension::ParserOverrideFunction;

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