#include "tee_extension.hpp"
#include "tee_parser.hpp"
#include "duckdb/common/csv_writer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/box_renderer.hpp"



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
		return;
	}
	const string tee = "Tee Pager: \n";
	fwrite(tee.data(), 1, tee.size(), pager_out);
	fwrite(out.data(), 1, out.size(), pager_out);
	pclose(pager_out);
	FinishPagerDisplay();
}

TeeGlobalState::TeeGlobalState(ClientContext &context, const vector<LogicalType> &types_p,
                               const vector<string> &names_p)
    : buffered(context, types_p), names(names_p), types(types_p), context_ptr(&context), printed_flag(false),
      parameter_flag(false), pager_flag(false), terminal_flag(true), symbol_flag(false), path_flag(false),
      table_name_flag(false), flushed(false) {
}

TeeGlobalState::~TeeGlobalState() {
	TeeFlushOutputs();
}

static void TeeCSVWriter(ClientContext &context, ColumnDataCollection &buffered, const vector<string> &names,
                         const string &path) {
	// write always in my local dir
	// path = "test_dir/csv_files_testing/" + path;
	std::cout << "Write to: " << path << "\n";

	FileSystem &fs = FileSystem::GetFileSystem(context);

	// prepare options
	CSVReaderOptions options;
	options.name_list = names;
	// set own names
	options.columns_set = true;
	options.force_quote.resize(names.size(), false);

	CSVWriterState write_state(context,
	                           4096ULL * 8ULL); // in csv_writer.hpp they used: idx_t flush_size = 4096ULL * 8ULL;
	// initialize writer
	CSVWriter writer(options, fs, path, FileCompressionType::UNCOMPRESSED, false);

	// force writing header and prefix
	writer.Initialize(true);

	// scan/initialize ColumnDataCollection with our buffered data
	// this is the place where our column data lives
	ColumnDataScanState scan_state;
	buffered.InitializeScan(scan_state);

	// initialize chunk
	DataChunk chunk;
	buffered.InitializeScanChunk(scan_state, chunk);

	// as long as we have data, write it
	while (buffered.Scan(scan_state, chunk)) {
		// CSVWriter expects varchar columns. For that, we cast our current chunk into a new chunk
		// which has only varchar columns.
		DataChunk varchar_chunk;
		vector<LogicalType> varchar_vector;
		varchar_vector.reserve(chunk.ColumnCount());
		for (idx_t i = 0; i < chunk.ColumnCount(); ++i) {
			varchar_vector.emplace_back(LogicalType::VARCHAR);
		}
		// initialize chunk with the same client context
		varchar_chunk.Initialize(context, varchar_vector);
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

static void TeeTableWriter(ClientContext &context, ColumnDataCollection &buffered, const vector<string> &names,
                           const vector<LogicalType> &types, const string &table_name) {
	auto &db = context.db->GetDatabase(context);
	Connection con(db);

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
	buffered.InitializeScan(scan_state);

	DataChunk chunk;
	buffered.InitializeScanChunk(scan_state, chunk);

	while (buffered.Scan(scan_state, chunk)) {
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

static OperatorResultType TeeTableFun(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                      DataChunk &output) {
	auto &global_state = data_p.global_state->Cast<TeeGlobalState>();

	lock_guard<mutex> guard(global_state.lock);
	global_state.buffered.Append(input);
	output.Reference(input);

	return OperatorResultType::NEED_MORE_INPUT;
}

void TeeGlobalState::TeeFlushOutputs() {
	lock_guard<mutex> guard(lock);
	if (flushed) {
		return;
	}
	flushed = true;

	if (pager_flag || terminal_flag) {
		BoxRendererConfig config;
		BoxRenderer renderer(config);

		if (symbol_flag && !pager_flag) {
			Printer::RawPrint(OutputStream::STREAM_STDOUT, "Tee Operator; Symbol:  " + symbol + "\n");
		} else if (!pager_flag) {
			Printer::RawPrint(OutputStream::STREAM_STDOUT, "Tee Operator: \n");
		}

		if (pager_flag) {
			string out_str = renderer.ToString(*context_ptr, names, buffered);
			SetupPager(out_str);
		} else {
			renderer.Print(*context_ptr, names, buffered);
		}
	}

	if (path_flag) {
		TeeCSVWriter(*context_ptr, buffered, names, path);
	}
	if (table_name_flag) {
		TeeTableWriter(*context_ptr, buffered, names, types, table_name);
	}
}

static unique_ptr<GlobalTableFunctionState> TeeInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<TeeBindData>();
	auto result = make_uniq<TeeGlobalState>(context, bind_data.types, bind_data.names);

	auto &parameter_map = bind_data.tee_named_parameters;
	result->parameter_flag = true;

	if (parameter_map.find("pager") != parameter_map.end()) {
		result->pager_flag = parameter_map.at("pager").GetValue<bool>();
	}

	if (parameter_map.find("terminal") != parameter_map.end()) {
		result->terminal_flag = parameter_map.at("terminal").GetValue<bool>();
	}

	if (parameter_map.find("symbol") != parameter_map.end()) {
		result->symbol_flag = true;
		result->symbol = parameter_map.at("symbol").GetValue<string>();
	}

	if (parameter_map.find("path") != parameter_map.end()) {
		result->path_flag = true;
		result->path = parameter_map.at("path").GetValue<string>();
	}

	if (parameter_map.find("table_name") != parameter_map.end()) {
		result->table_name_flag = true;
		result->table_name = parameter_map.at("table_name").GetValue<string>();
	}

	return result;
}

static unique_ptr<FunctionData> TeeBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	return_types = input.input_table_types;
	names = input.input_table_names;
	return make_uniq<TeeBindData>(names, return_types, input.named_parameters);
}

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction tee_function("__rewrite_query", {LogicalType::TABLE}, nullptr, TeeBind);
	tee_function.init_global = TeeInitGlobal;
	tee_function.in_out_function = TeeTableFun;
	tee_function.named_parameters["path"] = LogicalType::VARCHAR;
	tee_function.named_parameters["symbol"] = LogicalType::VARCHAR;
	tee_function.named_parameters["terminal"] = LogicalType::BOOLEAN;
	tee_function.named_parameters["table_name"] = LogicalType::VARCHAR;
	tee_function.named_parameters["pager"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(tee_function);

	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.SetOptionByName("allow_parser_override_extension", Value("fallback"));

	ParserExtension parser_extension;
	parser_extension.parser_override = TeeParserExtension::ParserOverrideFunction;
	ParserExtension::Register(config, std::move(parser_extension));
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