#include "tee_extension.hpp"
#include "tee_parser.hpp"
#include "duckdb/common/csv_writer.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

static string GetSystemPager() {
	const char *duckdb_pager = getenv("DUCKDB_PAGER");

	// Try DUCKDB_PAGER first (highest priority for env vars)
	if (duckdb_pager && strlen(duckdb_pager) > 0) {
		return duckdb_pager;
	}

	// Try PAGER next
	const char *pager = getenv("PAGER");
	if (pager && strlen(pager) > 0) {
		return pager;
	}

	// No valid pager environment variable set, use platform default
#if defined(_WIN32) || defined(WIN32)
	// On Windows, use 'more' as default pager
	return "more";
#else
	// On other systems, use 'less' as default pager
	return "less -SRX";
#endif
}

void StartPagerDisplay() {
#if !defined(_WIN32) && !defined(WIN32)
	// disable sigpipe trap while displaying the pager
	signal(SIGPIPE, SIG_IGN);
#endif
}

void FinishPagerDisplay() {
#if !defined(_WIN32) && !defined(WIN32)
	// enable sigpipe trap again after finishing the display
	signal(SIGPIPE, SIG_DFL);
#endif
}

void SetupPager(const string &out) {
	string sys_pager = GetSystemPager();
#if defined(_WIN32) || defined(WIN32)
	if (win_utf8_mode) {
		SetConsoleCP(CP_UTF8);
	}
#endif
	StartPagerDisplay();
	// open and write into pager
	auto pager_out = popen(sys_pager.c_str(), "w");
	if (!pager_out) {
		FinishPagerDisplay();
	}
	const string tee = "Tee Pager: \n";
	fwrite(tee.data(), 1, tee.size(), pager_out);
	fwrite(out.data(), 1, out.size(), pager_out);
	pclose(pager_out);
}

static void TeeCSVWriter(const ExecutionContext &context, TableFunctionInput &data_p, DataChunk &output, string &path) {
	auto &global_state = data_p.global_state->Cast<TeeGlobalState>();

	// write always in my local dir
	// path = "test_dir/csv_files_testing/" + path;
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

	string sql_statement = "CREATE TABLE IF NOT EXISTS " + table_name + "(" + name_types + ")";

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
	string out = "Table " + table_name + " created and added to the current attached database. \n";
	Printer::RawPrint(OutputStream::STREAM_STDOUT, out);
}

// this function is called once per chunk
static OperatorResultType TeeTableFun(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                      DataChunk &output) {
	// access to the global state object
	auto &global_state = data_p.global_state->Cast<TeeGlobalState>();
	auto &parameter_map = data_p.bind_data->Cast<TeeBindData>().tee_named_parameters;

	// lock the global_space to prevent race conditions
	// and append the current chunk to the global buffer
	global_state.lock.lock();
	global_state.buffered.Append(input);
	output.Reference(input);
	global_state.lock.unlock();

	bool pager_flag = false;
	if (parameter_map.find("pager") != parameter_map.end()) {
		pager_flag = parameter_map.at("pager").GetValue<bool>();
	}
	bool terminal_flag = true;
	if (parameter_map.find("terminal") != parameter_map.end()) {
		terminal_flag = parameter_map.at("terminal").GetValue<bool>();
	}

	// Render and print current chunk
	if (terminal_flag || pager_flag) {
		BoxRendererConfig config;
		// config.max_rows = 100;
		BoxRenderer renderer(config);
		vector<LogicalType> chunk_types;
		chunk_types.reserve(input.ColumnCount());
		for (idx_t i = 0; i < input.ColumnCount(); ++i) {
			chunk_types.emplace_back(input.data[i].GetType());
		}
		ColumnDataCollection chunk_collection(context.client, chunk_types);
		chunk_collection.Append(input);

		StringResultRenderer result_renderer;
		renderer.Render(context.client, global_state.names, chunk_collection, result_renderer);
		string out = renderer.ToString(context.client, global_state.names, chunk_collection);

		if (parameter_map.find("symbol") != parameter_map.end()) {
			string symbol = parameter_map.at("symbol").GetValue<string>();
			std::cout << "Tee Operator Query: " << symbol << "\n";
		} else if (!pager_flag) {
			std::cout << "Tee Operator: \n";
		}

		if (pager_flag) {
			SetupPager(out);
		} else if (terminal_flag) {
			Printer::RawPrint(OutputStream::STREAM_STDOUT, out);
		}
	}

	if (parameter_map.find("path") != parameter_map.end()) {
		string path = parameter_map.at("path").GetValue<string>();
		TeeCSVWriter(context, data_p, output, path);
	}

	if (parameter_map.find("table_name") != parameter_map.end()) {
		string table_name = parameter_map.at("table_name").GetValue<string>();
		TeeTableWriter(context, data_p, output, table_name);
	}

	return OperatorResultType::NEED_MORE_INPUT;
}
// this function is called once at the start of execution to create the global state
static unique_ptr<GlobalTableFunctionState> TeeInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<TeeBindData>();
	return make_uniq<TeeGlobalState>(context, bind_data.types, bind_data.names);
}

static unique_ptr<FunctionData> TeeBind(ClientContext &context, const TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	names = input.input_table_names;
	return_types = input.input_table_types;
	// returns a bind data object
	return make_uniq<TeeBindData>(names, return_types, input.named_parameters);
}

static unique_ptr<LogicalOperator> TeeBindOperator(ClientContext &context, TableFunctionBindInput &input,
                                                   idx_t bind_index, vector<string> &return_names) {
	return_names = input.input_table_names;
	return nullptr;
}

// called when the extension is loaded
// registers the tee table function and the parser extension
static void LoadInternal(ExtensionLoader &loader) {
	TableFunction tee_function("tee", {LogicalType::TABLE}, nullptr, reinterpret_cast<table_function_bind_t>(TeeBind));
	tee_function.init_global = TeeInitGlobal;
	tee_function.in_out_function = TeeTableFun;
	tee_function.bind_operator = TeeBindOperator;
	// tee_function.in_out_function_final = TeeFinalize;
	tee_function.named_parameters["path"] = LogicalType::VARCHAR;
	tee_function.named_parameters["symbol"] = LogicalType::VARCHAR;
	tee_function.named_parameters["terminal"] = LogicalType::BOOLEAN;
	tee_function.named_parameters["table_name"] = LogicalType::VARCHAR;
	tee_function.named_parameters["pager"] = LogicalType::BOOLEAN;
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