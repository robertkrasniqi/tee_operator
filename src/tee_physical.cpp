#include "include/tee_physical.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/box_renderer_context.hpp"
#include "duckdb/common/column_data_collection_render_interface.hpp"
#include "duckdb/common/csv_writer.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"

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

PhysicalTee::PhysicalTee(PhysicalPlan &physical_plan, vector<LogicalType> types_p, vector<string> names_p,
                         idx_t estimated_cardinality, named_parameter_map_t tee_named_parameters_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      names_output(std::move(names_p)), options(tee_named_parameters_p) {
}

// For EXPLAIN output
InsertionOrderPreservingMap<string> PhysicalTee::ParamsToString() const {
	InsertionOrderPreservingMap<string> out;

	if (options.terminal_flag) {
		out["terminal"] = "active";
	}
	if (options.pager_flag) {
		out["pager"] = "active";
	}
	if (options.symbol_flag) {
		out["symbol"] = options.symbol;
	}
	if (options.path_flag) {
		out["path"] = options.path;
	}
	if (options.table_name_flag) {
		out["table_name"] = options.table_name;
	}
	// maxrows is always shown
	if (options.max_rows == NumericLimits<idx_t>::Maximum()) {
		out["maxrows"] = "all";
	} else {
		out["maxrows"] = to_string(options.max_rows);
	}
	SetEstimatedCardinality(out, estimated_cardinality);
	return out;
}

TeeLocalState::TeeLocalState(ClientContext &context, const TeeOptions &options, const vector<LogicalType> &tee_types,
                             shared_ptr<TeeGlobalState> global_state_p)
    : global_state(std::move(global_state_p)) {
	if (options.NeedsBuffer()) {
		local_buffer = make_uniq<ColumnDataCollection>(context, tee_types);
		local_buffer->InitializeAppend(local_append_state);
	}
	// inside a recursive CTE, targets get an extra column for the iteration step
	int const iteration_column = global_state->RecursiveIteration() ? 1 : 0;

	if (options.path_flag) {
		vector<LogicalType> varchar_types(tee_types.size() + iteration_column, LogicalType::VARCHAR);
		varchar_chunk_csv.Initialize(context, varchar_types);
		// in csv_writer.hpp they used: idx_t flush_size = 4096ULL * 8ULL;
		local_csv_state = make_uniq<CSVWriterState>(context, 4096ULL * 8ULL);
	}
	if (options.table_name_flag && global_state->RecursiveIteration()) {
		vector<LogicalType> table_types;
		table_types.push_back(LogicalType::BIGINT);
		table_types.insert(table_types.end(), tee_types.begin(), tee_types.end());
		chunk_with_iteration_column.Initialize(context, table_types);
	}
}

void TeeLocalState::Finalize(const PhysicalOperator &op, ExecutionContext &context) {
	if (local_buffer) {
		global_state->AppendLocalToGlobalBuffer(*local_buffer);
	}
}

void TeeLocalState::Reset() {
	if (local_buffer) {
		local_buffer->InitializeAppend(local_append_state);
	}
}

unique_ptr<OperatorState> PhysicalTee::GetOperatorState(ExecutionContext &context) const {
	auto key = StateKey();
	auto global_state = context.client.registered_state->GetOrCreate<TeeGlobalState>(
	    key, context.client, options, names_output, types, key, is_recursive_cte);
	return make_uniq<TeeLocalState>(context.client, options, types, std::move(global_state));
}

OperatorResultType PhysicalTee::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                        GlobalOperatorState &global_state, OperatorState &state) const {
	auto &l_state = state.Cast<TeeLocalState>();

	// Buffer
	if (l_state.local_buffer) {
		l_state.local_buffer->Append(l_state.local_append_state, input);
	}
	// Stream
	if (options.NeedsStream()) {
		l_state.global_state->WriteChunk(context.client, input, l_state);
	}
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

TeeGlobalState::TeeGlobalState(ClientContext &context, const TeeOptions &options, const vector<string> &names,
                               const vector<LogicalType> &types, string key_p, bool recursive_iteration_p)
    : recursive_iteration(recursive_iteration_p), key(std::move(key_p)) {
	if (options.NeedsBuffer()) {
		buffered = make_uniq<ColumnDataCollection>(context, types);
	}
	if (options.path_flag) {
		TeeInitializeCSVWriter(context, options, names);
	}
	if (options.table_name_flag) {
		TeeInitializeTableWriter(context, options, names, types);
	}
}

void TeeGlobalState::TeeInitializeCSVWriter(ClientContext &context, const TeeOptions &options,
                                            const vector<string> &names) {
	Printer::Print(OutputStream::STREAM_STDOUT, "Write to: " + options.path);
	FileSystem &fs = FileSystem::GetFileSystem(context);

	vector<Identifier> csv_column_names;
	csv_column_names.reserve(names.size() + 1);
	// insert iteration column in recursive CTEs
	if (recursive_iteration) {
		csv_column_names.push_back("iteration");
	}
	for (const auto &name : names) {
		csv_column_names.push_back(Identifier(name));
	}

	// prepare options
	CSVReaderOptions csv_options;
	csv_options.name_list = csv_column_names;
	// set own names
	csv_options.columns_set = true;
	csv_options.force_quote.resize(csv_column_names.size(), false);

	csv_writer = make_uniq<CSVWriter>(csv_options, fs, options.path, FileCompressionType::UNCOMPRESSED);
	// force writing header and prefix
	csv_writer->Initialize(true);
}

void TeeGlobalState::TeeInitializeTableWriter(ClientContext &context, const TeeOptions &options,
                                              const vector<string> &names, const vector<LogicalType> &types) {
	auto &db = context.db->GetDatabase(context);
	con = make_uniq<Connection>(db);

	// copy the name and type schema of the current subquery for the new table
	string name_types = recursive_iteration ? " iteration BIGINT" : "";
	for (idx_t i = 0; i < names.size(); i++) {
		if (!name_types.empty()) {
			name_types += ", ";
		}
		// quote the name, a column can be called "count_star()" or "select"
		name_types += " " + SQLIdentifier::ToString(names[i]) + " " + types[i].ToString();
	}
	auto create = con->Query("CREATE TABLE IF NOT EXISTS " + SQLIdentifier::ToString(options.table_name) + "(" +
	                         name_types + ")");
	if (create->HasError()) {
		create->GetErrorObject().Throw();
	}

	// create an appender on the existing context
	// is responsible for writing the actual rows in the table
	appender = make_uniq<Appender>(*con, Identifier(options.table_name));
	Printer::Print(OutputStream::STREAM_STDOUT,
	               "Table " + options.table_name + " created and added to the current attached database. ");
}

void TeeGlobalState::QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) {
	if (appender) {
		appender->Close();
		appender.reset();
	}
	if (csv_writer) {
		csv_writer->Close();
		csv_writer.reset();
	}

	context.registered_state->Remove(key);
}

void TeeGlobalState::WriteChunk(ClientContext &context, DataChunk &chunk, TeeLocalState &l_state) {
	idx_t rows = chunk.size();
	if (rows == 0) {
		return;
	}
	idx_t offset = recursive_iteration ? 1 : 0;
	idx_t step = CurrentIteration();

	if (csv_writer) {
		auto &varchar_chunk = l_state.varchar_chunk_csv;
		varchar_chunk.Reset();
		if (recursive_iteration) {
			varchar_chunk.data[0].Reference(Value(to_string(step)), count_t(rows));
		}
		for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
			VectorOperations::Cast(context, chunk.data[col], varchar_chunk.data[col + offset], rows);
		}
		varchar_chunk.SetChildCardinality(rows);

		csv_writer->WriteChunk(varchar_chunk, *l_state.local_csv_state);
		csv_writer->Flush(*l_state.local_csv_state);
	}

	// Write chunk to table
	if (appender) {
		if (!recursive_iteration) {
			lock_guard<mutex> guard(appender_lock);
			appender->AppendDataChunk(chunk);
			return;
		}
		auto &table_chunk = l_state.chunk_with_iteration_column;
		table_chunk.Reset();
		table_chunk.data[0].Reference(Value::BIGINT(NumericCast<int64_t>(step)), count_t(rows));
		for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
			table_chunk.data[col + 1].Reference(chunk.data[col]);
		}
		table_chunk.SetChildCardinality(rows);

		lock_guard<mutex> guard(appender_lock);
		appender->AppendDataChunk(table_chunk);
	}
}

void TeeGlobalState::Flush() {
	if (appender) {
		lock_guard<mutex> guard(appender_lock);
		appender->Flush();
	}
}

// called at the end of a pipeline
OperatorFinalResultType PhysicalTee::OperatorFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                      OperatorFinalizeInput &input) const {
	auto tee_state = context.registered_state->Get<TeeGlobalState>(StateKey());

	tee_state->Flush();

	if (!options.NeedsBuffer()) {
		tee_state->NextIteration();
		return OperatorFinalResultType::FINISHED;
	}

	ColumnDataCollectionWrapper render_buffer(*tee_state->buffered);
	ClientBoxRendererContext render_context(context);
	BoxRendererConfig config;
	config.max_rows = options.max_rows;
	BoxRenderer renderer(config);
	string str_out = renderer.ToString(render_context, names_output, render_buffer);

	if (options.symbol_flag && !options.pager_flag) {
		Printer::Print(OutputStream::STREAM_STDOUT, "Tee Operator; Symbol: " + options.symbol);
	} else if (!options.pager_flag) {
		Printer::Print(OutputStream::STREAM_STDOUT, "Tee Operator: ");
	}
	if (options.pager_flag) {
		SetupPager(str_out);
	} else {
		Printer::RawPrint(OutputStream::STREAM_STDOUT, str_out);
	}

	Printer::Flush(OutputStream::STREAM_STDOUT);

	tee_state->ResetBuffer();
	tee_state->NextIteration();

	return OperatorFinalResultType::FINISHED;
}
} // namespace duckdb
