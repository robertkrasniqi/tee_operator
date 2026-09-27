#include "tee_physical.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/box_renderer_context.hpp"
#include "duckdb/common/column_data_collection_render_interface.hpp"
#include "duckdb/common/csv_writer.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"

namespace duckdb {

PhysicalTee::PhysicalTee(PhysicalPlan &physical_plan, vector<LogicalType> types_p, vector<string> names_p,
                         idx_t estimated_cardinality, const named_parameter_map_t &tee_named_parameters_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      names_output(std::move(names_p)), tee_options(tee_named_parameters_p) {
}

// For EXPLAIN output
InsertionOrderPreservingMap<string> PhysicalTee::ParamsToString() const {
	InsertionOrderPreservingMap<string> out;

	if (tee_options.has_terminal) {
		out["terminal"] = "active";
	}
	if (tee_options.has_pager) {
		out["pager"] = "active";
	}
	if (tee_options.has_symbol) {
		out["symbol"] = tee_options.symbol;
	}
	if (tee_options.has_path) {
		out["path"] = tee_options.path;
	}
	if (tee_options.has_table) {
		out["table_name"] = tee_options.table_name;
	}
	// maxrows is always shown
	if (tee_options.max_rows == NumericLimits<idx_t>::Maximum()) {
		out["maxrows"] = "all";
	} else {
		out["maxrows"] = to_string(tee_options.max_rows);
	}
	SetEstimatedCardinality(out, estimated_cardinality);
	return out;
}

TeeLocalState::TeeLocalState(ClientContext &context, const TeeOptions &options, const vector<LogicalType> &tee_types,
                             bool is_recursive_cte) {
	if (options.has_terminal || options.has_pager) {
		// Prepare local buffer
		local_buffer = make_uniq<ColumnDataCollection>(context, tee_types);
		local_buffer->InitializeAppend(local_append_state);
	}
	// Inside a recursive CTE, targets get an extra column for the iteration step
	const idx_t iteration_column = is_recursive_cte ? 1 : 0;

	// Prepare the local CSV state and the VARCHAR chunk we need
	if (options.has_path) {
		vector<LogicalType> varchar_types(tee_types.size() + iteration_column, LogicalType::VARCHAR);
		varchar_chunk_csv.Initialize(context, varchar_types);
		local_csv_state = make_uniq<CSVWriterState>(context, 4096ULL * 8ULL);
	}

	// Prepare the chunk we use to write to tables
	if (options.has_table && is_recursive_cte) {
		vector<LogicalType> table_types;
		table_types.push_back(LogicalType::BIGINT);
		table_types.insert(table_types.end(), tee_types.begin(), tee_types.end());
		chunk_with_iteration_column.Initialize(context, table_types);
	}
}

// Depending on the TeeOptions the use chose, the input chunk gets:
// - appended to the local buffer
// - written to the csv
// - written to the table
OperatorResultType PhysicalTee::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                        GlobalOperatorState &global_state, OperatorState &state) const {
	auto &l_state = state.Cast<TeeLocalState>();

	if (l_state.local_buffer) {
		l_state.local_buffer->Append(l_state.local_append_state, input);
	}
	if (tee_options.has_path) {
		global_state.Cast<TeeGlobalState>().WriteChunkToCSV(context.client, input, l_state);
	}
	if (tee_options.has_table) {
		global_state.Cast<TeeGlobalState>().WriteChunkToTable(context.client, input, l_state);
	}
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

TeeGlobalState::TeeGlobalState(ClientContext &context, const TeeOptions &options, const vector<string> &names,
                               const vector<LogicalType> &types, bool recursive_iteration_p)
    : is_recursive_cte(recursive_iteration_p) {
	if (options.has_terminal || options.has_pager) {
		// If neither is set, we don't need a buffer and rely on streaming completely
		buffered = make_uniq<ColumnDataCollection>(context, types);
	}
	if (options.has_path) {
		TeeInitializeCSVWriter(context, options, names);
	}
	if (options.has_table) {
		TeeInitializeTableWriter(context, options, names, types);
	}
}

TeeGlobalState::~TeeGlobalState() {
	// The csv writer does not close itself, so we have to close it
	if (csv_writer) {
		csv_writer->Close();
	}
}

void TeeGlobalState::TeeInitializeCSVWriter(ClientContext &context, const TeeOptions &options,
                                            const vector<string> &names) {
	Printer::Print(OutputStream::STREAM_STDOUT, "Write to: " + options.path);

	FileSystem &fs = FileSystem::GetFileSystem(context);
	vector<Identifier> csv_column_names;
	csv_column_names.reserve(names.size() + 1);
	// insert iteration column in recursive CTEs
	if (is_recursive_cte) {
		csv_column_names.push_back("iteration");
	}
	for (const auto &name : names) {
		csv_column_names.push_back(Identifier(name));
	}

	// prepare options
	CSVReaderOptions csv_options;
	csv_options.name_list = csv_column_names;
	csv_options.columns_set = true;
	csv_options.force_quote.resize(csv_column_names.size(), false);

	csv_writer = make_uniq<CSVWriter>(csv_options, fs, options.path, FileCompressionType::UNCOMPRESSED);
	csv_writer->Initialize(true);
}

void TeeGlobalState::TeeInitializeTableWriter(ClientContext &context, const TeeOptions &options,
                                              const vector<string> &names, const vector<LogicalType> &types) {
	auto &db = context.db->GetDatabase(context);
	con = make_uniq<Connection>(db);

	// copy the name and type schema of the current subquery for the new table
	vector<string> columns;
	if (is_recursive_cte) {
		columns.reserve(names.size() + 1);
		columns.push_back("iteration BIGINT");
	} else {
		columns.reserve(names.size());
	}

	for (idx_t i = 0; i < names.size(); i++) {
		// Quote column names
		columns.push_back(SQLIdentifier::ToString(names[i]) + " " + types[i].ToString());
	}
	const auto create = con->Query("CREATE TABLE IF NOT EXISTS " + SQLIdentifier::ToString(options.table_name) + "(" +
	                               StringUtil::Join(columns, ", ") + ")");

	appender = make_uniq<Appender>(*con, Identifier(options.table_name));
	Printer::Print(OutputStream::STREAM_STDOUT,
	               "Table " + options.table_name + " created and added to the current attached database. ");
}

void TeeGlobalState::WriteChunkToCSV(ClientContext &context, DataChunk &chunk, TeeLocalState &l_state) const {
	const idx_t rows = chunk.size();

	if (rows == 0) {
		return;
	}

	const idx_t offset = is_recursive_cte ? 1 : 0;

	auto &varchar_chunk = l_state.varchar_chunk_csv;
	varchar_chunk.Reset();
	if (is_recursive_cte) {
		const idx_t step = CurrentIteration();
		varchar_chunk.data[0].Reference(Value(to_string(step)), count_t(rows));
	}

	for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
		VectorOperations::Cast(context, chunk.data[col], varchar_chunk.data[col + offset], rows);
	}
	varchar_chunk.SetChildCardinality(rows);

	csv_writer->WriteChunk(varchar_chunk, *l_state.local_csv_state);
	csv_writer->Flush(*l_state.local_csv_state);
}

void TeeGlobalState::WriteChunkToTable(ClientContext &context, DataChunk &chunk, TeeLocalState &l_state) {
	const idx_t rows = chunk.size();
	if (rows == 0) {
		return;
	}
	const auto step = NumericCast<int64_t>(CurrentIteration());

	if (!is_recursive_cte) {
		lock_guard<mutex> guard(appender_lock);
		appender->AppendDataChunk(chunk);
		return;
	}

	auto &table_chunk = l_state.chunk_with_iteration_column;
	table_chunk.Reset();
	table_chunk.data[0].Reference(Value::BIGINT(step), count_t(rows));
	for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
		table_chunk.data[col + 1].Reference(chunk.data[col]);
	}
	table_chunk.SetChildCardinality(rows);

	lock_guard<mutex> guard(appender_lock);
	appender->AppendDataChunk(table_chunk);
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

static void StartPagerDisplay() {
#if !defined(_WIN32) && !defined(WIN32)
	// disable sigpipe trap while displaying the pager
	signal(SIGPIPE, SIG_IGN);
#endif
}

static void FinishPagerDisplay() {
#if !defined(_WIN32) && !defined(WIN32)
	// enable sigpipe trap again after finishing the display
	signal(SIGPIPE, SIG_DFL);
#endif
}

static void SetupPager(const string &out) {
	string sys_pager = GetSystemPager();
#if defined(_WIN32) || defined(WIN32)
	SetConsoleCP(CP_UTF8);
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

// called at the end of a pipeline
OperatorFinalResultType PhysicalTee::OperatorFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                      OperatorFinalizeInput &input) const {
	auto &g_state = input.global_state.Cast<TeeGlobalState>();

	if (!tee_options.has_terminal && !tee_options.has_pager) {
		// No terminal or pager -> already finished, return
		g_state.NextIteration();
		return OperatorFinalResultType::FINISHED;
	}

	ColumnDataCollectionWrapper render_buffer(*g_state.buffered);
	ClientBoxRendererContext render_context(context);
	BoxRendererConfig config;

	config.max_rows = tee_options.max_rows;
	BoxRenderer renderer(config);

	string str_out = renderer.ToString(render_context, names_output, render_buffer);

	if (tee_options.has_symbol && !tee_options.has_pager) {
		Printer::Print(OutputStream::STREAM_STDOUT, "Tee Operator; Symbol: " + tee_options.symbol);
	} else if (!tee_options.has_pager) {
		Printer::Print(OutputStream::STREAM_STDOUT, "Tee Operator: ");
	}
	if (tee_options.has_pager) {
		SetupPager(str_out);
	} else {
		Printer::RawPrint(OutputStream::STREAM_STDOUT, str_out);
	}

	Printer::Flush(OutputStream::STREAM_STDOUT);

	g_state.buffered->ResetForReuse();

	g_state.NextIteration();

	return OperatorFinalResultType::FINISHED;
}
} // namespace duckdb
