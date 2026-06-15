#include "include/tee_physical.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/box_renderer_context.hpp"
#include "duckdb/common/column_data_collection_render_interface.hpp"
#include "duckdb/common/csv_writer.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"

namespace duckdb {

PhysicalTee::PhysicalTee(PhysicalPlan &physical_plan, vector<LogicalType> types_p, vector<string> names_p,
                         idx_t estimated_cardinality, idx_t projected_input_count_p,
                         named_parameter_map_t tee_named_parameters_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      names_output(std::move(names_p)), projected_input_count(projected_input_count_p),
      tee_named_parameters(std::move(tee_named_parameters_p)) {
}

unique_ptr<GlobalOperatorState> PhysicalTee::GetGlobalOperatorState(ClientContext &context) const {
	idx_t original_col_count = types.size() - projected_input_count;
	return make_uniq<TeeGlobalState>(context, types, names_output, original_col_count, tee_named_parameters);
}

OperatorResultType PhysicalTee::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                        GlobalOperatorState &global_state, OperatorState &state) const {
	auto &tee_state = global_state.Cast<TeeGlobalState>();
	idx_t original_col_count = tee_state.all_col_count;

	// no correlation -> normal buffering
	if (projected_input_count == 0) {
		{
			lock_guard<mutex> guard(tee_state.lock);
			tee_state.buffered.Append(input);
		}
		chunk.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}
	// correlated case:
	// we only want to display (and buffer) the original cols, but pass the full input (which includes projected_input)
	{
		lock_guard<mutex> guard(tee_state.lock);
		// build a chunk that references only the first original columns
		DataChunk original_chunk;
		const vector<LogicalType> original_types(types.begin(), types.begin() + original_col_count);
		original_chunk.InitializeEmpty(original_types);
		for (idx_t i = 0; i < original_col_count; i++) {
			original_chunk.data[i].Reference(input.data[i]);
		}
		original_chunk.SetCardinality(input.size());
		tee_state.buffered.Append(original_chunk);
	}
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

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

// writes the tee output into a CSV file into the passed path
static void TeeCSVWriter(ClientContext &context, ColumnDataCollection &buffered, const vector<string> &names,
                         const string &path) {
	// write always in my local dir
	string path_testing = "test_dir/csv_files_testing/" + path;
	Printer::RawPrint(OutputStream::STREAM_STDOUT, "Write to: " + path_testing + "\n");
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
	CSVWriter writer(options, fs, path_testing, FileCompressionType::UNCOMPRESSED, false);

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

// write the tee output into a table with the passed name
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

OperatorFinalResultType PhysicalTee::OperatorFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                      OperatorFinalizeInput &input) const {
	auto &tee_state = input.global_state.Cast<TeeGlobalState>();

	ColumnDataCollectionWrapper render_buffer(tee_state.buffered);
	ClientBoxRendererContext render_context(context);
	BoxRendererConfig config;
	BoxRenderer renderer(config);
	string str_out = renderer.ToString(render_context, tee_state.names, render_buffer);

	if (tee_state.pager_flag || tee_state.terminal_flag) {
		if (tee_state.symbol_flag && !tee_state.pager_flag) {
			Printer::RawPrint(OutputStream::STREAM_STDOUT, "Tee Operator; Symbol: " + tee_state.symbol + "\n");
		} else if (!tee_state.pager_flag) {
			Printer::RawPrint(OutputStream::STREAM_STDOUT, "Tee Operator: \n");
		}
		if (tee_state.pager_flag) {
			SetupPager(str_out);
		} else {
			Printer::RawPrint(OutputStream::STREAM_STDOUT, str_out);
		}
	}
	if (tee_state.path_flag) {
		TeeCSVWriter(context, tee_state.buffered, tee_state.names, tee_state.path);
	}
	if (tee_state.table_name_flag) {
		TeeTableWriter(context, tee_state.buffered, tee_state.names, types, tee_state.table_name);
	}

	return OperatorFinalResultType::FINISHED;
}
} // namespace duckdb
