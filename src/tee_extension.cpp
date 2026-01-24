#include "tee_extension.hpp"
#include "tee_parser.hpp"
#include "tee_physical.hpp"
#include "tee_logical.hpp"
#include "duckdb/common/csv_writer.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/planner/operator_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/exception/binder_exception.hpp"

#include <utility>
#include <duckdb/parser/tableref/table_function_ref.hpp>

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
      table_name_flag(false) {
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

// When the destructor is called, we know that the pipeline is done, and we can print / call pager.
TeeGlobalState::~TeeGlobalState() {
	if (printed_flag) {
		return;
	}
	printed_flag = true;
	if (pager_flag || terminal_flag) {
		BoxRendererConfig config;
		// config.max_rows = 100;
		BoxRenderer renderer(config);

		// pager doesnt print so symbol should not be printed either
		if (symbol_flag && !pager_flag) {
			Printer::RawPrint(OutputStream::STREAM_STDOUT, "Tee Operator Query: " + symbol + "\n");
		} else if (!pager_flag) {
			Printer::RawPrint(OutputStream::STREAM_STDOUT, "Tee Operator: \n");
		}
		if (pager_flag) {
			string out = renderer.ToString(*context_ptr, names, buffered);
			// Call pager
			SetupPager(out);
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

// this function is called once per chunk
static OperatorResultType TeeTableFun(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                      DataChunk &output) {
	auto &global_state = data_p.global_state->Cast<TeeGlobalState>();
	auto &parameter_map = data_p.bind_data->Cast<TeeBindData>().tee_named_parameters;

	// lock the global_state to prevent race conditions
	// and handle the current chunk
	global_state.lock.lock();

	// remember parameters once (if there are any parameters)
	if (!global_state.parameter_flag) {
		global_state.parameter_flag = true;

		global_state.pager_flag = false;
		if (parameter_map.find("pager") != parameter_map.end()) {
			global_state.pager_flag = parameter_map.at("pager").GetValue<bool>();
		}

		global_state.terminal_flag = true;
		if (parameter_map.find("terminal") != parameter_map.end()) {
			global_state.terminal_flag = parameter_map.at("terminal").GetValue<bool>();
		}

		global_state.symbol_flag = false;
		if (parameter_map.find("symbol") != parameter_map.end()) {
			global_state.symbol_flag = true;
			global_state.symbol = parameter_map.at("symbol").GetValue<string>();
		}

		global_state.path_flag = false;
		if (parameter_map.find("path") != parameter_map.end()) {
			global_state.path_flag = true;
			global_state.path = parameter_map.at("path").GetValue<string>();
		}

		global_state.table_name_flag = false;
		if (parameter_map.find("table_name") != parameter_map.end()) {
			global_state.table_name_flag = true;
			global_state.table_name = parameter_map.at("table_name").GetValue<string>();
		}
	}
	// buffer the chunk
	global_state.buffered.Append(input);
	output.Reference(input);

	global_state.lock.unlock();
	return OperatorResultType::NEED_MORE_INPUT;
}

// this function is called once at the start of execution to create the global state
static unique_ptr<GlobalTableFunctionState> TeeInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<TeeBindData>();
	return make_uniq<TeeGlobalState>(context, bind_data.types, bind_data.names);
}

static unique_ptr<FunctionData> TeeBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	return_types = input.input_table_types;
	names = input.input_table_names;
	return make_uniq<TeeBindData>(names, return_types, input.named_parameters);
}

static unique_ptr<LogicalOperator> TeeBindOperator(ClientContext &context, TableFunctionBindInput &input,
                                                   idx_t bind_index, vector<string> &return_names) {
	auto return_types = input.input_table_types;
	auto names = input.input_table_names;

	auto alias_names = input.ref.column_name_alias;
	vector<string> alias_table_name = {input.ref.alias};

	if (alias_names.empty()) {
		return_names = names;
	} else {
		return_names = alias_names;
	}

	auto bind_data = make_uniq<TeeBindData>(names, return_types, input.named_parameters);
	auto get = make_uniq<LogicalGet>(bind_index, input.table_function, std::move(bind_data), return_types, names,
	                                 virtual_column_map_t());

	for (auto &str : input.ref.column_name_alias) {
		std::cout << str << "\n";
	}

	get->parameters = input.inputs;
	get->named_parameters = input.named_parameters;
	get->input_table_types = input.input_table_types;

	if (alias_table_name.empty()) {
		get->input_table_names = input.input_table_names;
	} else {
		get->input_table_names = alias_table_name;
	}

	auto tee = make_uniq<LogicalTeeOperator>();
	tee->children.push_back(std::move(get));
	tee->names = names;

	return tee;
}

unique_ptr<TableRef> TeeBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	return nullptr;
}

// called when the extension is loaded
// registers the tee table function and the parser extension
static void LoadInternal(ExtensionLoader &loader) {
	TableFunction tee_function("tee", {LogicalType::TABLE}, nullptr, TeeBind);
	tee_function.init_global = TeeInitGlobal;
	tee_function.in_out_function = TeeTableFun;
	tee_function.bind_operator = TeeBindOperator;
	tee_function.projection_pushdown = true;
	tee_function.filter_pushdown = true;
	tee_function.bind_replace = TeeBindReplace;
	// tee_function.in_out_function_finasl = TeeFinalize;
	tee_function.named_parameters["path"] = LogicalType::VARCHAR;
	tee_function.named_parameters["symbol"] = LogicalType::VARCHAR;
	tee_function.named_parameters["terminal"] = LogicalType::BOOLEAN;
	tee_function.named_parameters["table_name"] = LogicalType::VARCHAR;
	tee_function.named_parameters["pager"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(tee_function);

	/* disable the parser for now
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.options.allow_parser_override_extension = "fallback";
	TeeParserExtension TeeExtensionParser;
	config.parser_extensions.push_back(TeeExtensionParser);
	*/
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