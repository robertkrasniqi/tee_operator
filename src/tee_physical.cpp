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
                         idx_t estimated_cardinality, idx_t projected_input_count_p,
                         named_parameter_map_t tee_named_parameters_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      names_output(std::move(names_p)), projected_input_count(projected_input_count_p), options(tee_named_parameters_p),
      tee_types(types.begin(), types.begin() + (types.size() - projected_input_count_p)) {
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
	if (options.path_flag) {
		vector<LogicalType> varchar_types(tee_types.size(), LogicalType::VARCHAR);
		varchar_chunk_csv.Initialize(context, varchar_types);
		// in csv_writer.hpp they used: idx_t flush_size = 4096ULL * 8ULL;
		local_csv_state = make_uniq<CSVWriterState>(context, 4096ULL * 8ULL);
	}
}

void TeeLocalState::Finalize(const PhysicalOperator &op, ExecutionContext &context) {
	if (local_buffer) {
		global_state->AppendLocalToGlobalBuffer(*local_buffer);
	}
}

void TeeLocalState::Reset() {
	if (local_buffer) {
		local_buffer->Reset();
		local_buffer->InitializeAppend(local_append_state);
	}
	if (local_csv_state) {
		local_csv_state->Reset();
	}
}

unique_ptr<OperatorState> PhysicalTee::GetOperatorState(ExecutionContext &context) const {
	string key = to_string(reinterpret_cast<uintptr_t>(this));
	auto global_state = context.client.registered_state->GetOrCreate<TeeGlobalState>(key, context.client, options,
	                                                                                 names_output, tee_types, key);
	return make_uniq<TeeLocalState>(context.client, options, tee_types, std::move(global_state));
}

OperatorResultType PhysicalTee::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                        GlobalOperatorState &global_state, OperatorState &state) const {
	auto &l_state = state.Cast<TeeLocalState>();

	DataChunk projected_chunk;
	optional_ptr<DataChunk> tee_chunk = input;
	if (projected_input_count > 0) {
		projected_chunk.InitializeEmpty(tee_types);
		for (idx_t i = 0; i < tee_types.size(); i++) {
			projected_chunk.data[i].Reference(input.data[i]);
		}
		projected_chunk.SetChildCardinality(input.size());
		tee_chunk = projected_chunk;
	}

	// Buffer
	if (l_state.local_buffer) {
		l_state.local_buffer->Append(l_state.local_append_state, *tee_chunk);
	}
	// Stream
	if (options.NeedsStream()) {
		l_state.global_state->WriteChunk(context.client, *tee_chunk, l_state);
	}
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

// Opens every streamed target once, they stay open until QueryEnd
TeeGlobalState::TeeGlobalState(ClientContext &context, const TeeOptions &options, const vector<string> &names,
                               const vector<LogicalType> &types, string key_p)
    : key(std::move(key_p)) {
	if (options.NeedsBuffer()) {
		buffered = make_uniq<ColumnDataCollection>(context, types);
	}
	if (options.path_flag) {
		TeeInitializeCSVWriter(context, options, names);
	}
	if (options.table_name_flag) {
		TeeInitializeTableWriter(context, options, names, types);
	}
	Printer::Flush(OutputStream::STREAM_STDOUT);
}

void TeeGlobalState::TeeInitializeCSVWriter(ClientContext &context, const TeeOptions &options,
                                            const vector<string> &names) {
	Printer::Print(OutputStream::STREAM_STDOUT, "Write to: " + options.path);
	FileSystem &fs = FileSystem::GetFileSystem(context);

	// prepare options
	CSVReaderOptions csv_options;
	csv_options.name_list = names;
	// set own names
	csv_options.columns_set = true;
	csv_options.force_quote.resize(names.size(), false);

	csv_writer = make_uniq<CSVWriter>(csv_options, fs, options.path, FileCompressionType::UNCOMPRESSED);
	// force writing header and prefix
	csv_writer->Initialize(true);
}

void TeeGlobalState::TeeInitializeTableWriter(ClientContext &context, const TeeOptions &options,
                                              const vector<string> &names, const vector<LogicalType> &types) {
	auto &db = context.db->GetDatabase(context);
	con = make_uniq<Connection>(db);

	// copy the name and type schema of the current subquery for the new table
	string name_types = "";
	for (idx_t i = 0; i < names.size(); i++) {
		name_types += " " + names[i] + " " + types[i].ToString();
		if (i + 1 < names.size()) {
			name_types += ", ";
		};
	}
	con->Query("CREATE TABLE IF NOT EXISTS " + options.table_name + "(" + name_types + ")");

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
	if (csv_writer) {
		auto &varchar_chunk = l_state.varchar_chunk_csv;
		varchar_chunk.Reset();
		for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
			VectorOperations::Cast(context, chunk.data[col], varchar_chunk.data[col], rows);
		}
		varchar_chunk.SetChildCardinality(rows);

		csv_writer->WriteChunk(varchar_chunk, *l_state.local_csv_state);
		csv_writer->Flush(*l_state.local_csv_state);
	}

	if (appender) {
		lock_guard<mutex> guard(appender_lock);
		appender->AppendDataChunk(chunk);
	}
}

void TeeGlobalState::Flush() {
	if (appender) {
		lock_guard<mutex> guard(appender_lock);
		appender->Flush();
	}
}

OperatorFinalResultType PhysicalTee::OperatorFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                      OperatorFinalizeInput &input) const {
	auto tee_state = context.registered_state->Get<TeeGlobalState>(StateKey());

	tee_state->Flush();

	if (!options.NeedsBuffer()) {
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

	return OperatorFinalResultType::FINISHED;
}
} // namespace duckdb
