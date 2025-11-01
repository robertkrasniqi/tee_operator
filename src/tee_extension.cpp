#include "tee_extension.hpp"
#include "tee_parser.hpp"
#include <regex>
#include "duckdb/common/csv_writer.hpp"


namespace duckdb {

// this function is called once per chunk
static OperatorResultType TeeTableFun(ExecutionContext &context,
                                      TableFunctionInput &data_p,
                                      DataChunk &input,
                                      DataChunk &output) {
	// access to the global state object
	auto &global_state = data_p.global_state->Cast<TeeGlobalState>();

	// append the current chunk to the global buffer
	global_state.buffered.Append(input);

	// pass through the input chunk unchanged to the output
	output.Reference(input);

	return OperatorResultType::NEED_MORE_INPUT;
}

// this function is called once at the end
// prints our bufferd data
static OperatorFinalizeResultType TeeFinalize(ExecutionContext &context,
                                              TableFunctionInput &data_p,
                                              DataChunk &output) {

	auto &global_state = data_p.global_state->Cast<TeeGlobalState>();

	// prints only once
	if (!global_state.printed) {
		std::cout << "Tee Operator:" << "\n";

		auto renderer = BoxRenderer();

		renderer.Print(context.client, global_state.names, global_state.buffered);

		global_state.printed = true;
	}
	return OperatorFinalizeResultType::FINISHED;
}

static OperatorFinalizeResultType TeeFinalizeS(ExecutionContext &context,
											  TableFunctionInput &data_p,
											  DataChunk &output) {

	auto &global_state = data_p.global_state->Cast<TeeGlobalState>();
	auto &bind_state = data_p.bind_data->Cast<TeeBindDataS>();
	// prints only once
	if (!global_state.printed) {
		std::cout << "Tee Operator, Query: " << bind_state.symbol << "\n";



		auto renderer = BoxRenderer();

		renderer.Print(context.client, global_state.names, global_state.buffered);

		global_state.printed = true;
	}
	return OperatorFinalizeResultType::FINISHED;
}

static OperatorFinalizeResultType TeeFinalizeC(ExecutionContext &context,
											  TableFunctionInput &data_p,
											  DataChunk &output) {

	auto &global_state = data_p.global_state->Cast<TeeGlobalState>();
	auto &bind_state = data_p.bind_data->Cast<TeeBindDataC>();

	if (!global_state.printed) {
		std::cout << "Tee Operator:" << "\n";

		if (!bind_state.path.empty()) {
			std::cout << "write to: " << bind_state.path << "\n";

			FileSystem &fs = FileSystem::GetFileSystem(context.client);

			// prepare options
			CSVReaderOptions options;
			options.name_list = global_state.names;
			// set own names
			options.columns_set = true;
			options.force_quote.resize(global_state.names.size(), false);

			// initialize state
			CSVWriterState write_state(context.client, 4096ULL * 8ULL); // in csv_writer.hpp they used: idx_t flush_size = 4096ULL * 8ULL;
			// initialize writer
			CSVWriter writer(options, fs, bind_state.path, FileCompressionType::UNCOMPRESSED, false);

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
	}
	auto renderer = BoxRenderer();
	renderer.Print(context.client, global_state.names, global_state.buffered);
	global_state.printed = true;

	return OperatorFinalizeResultType::FINISHED;
}

// this function is called once at the start of execution to create the global state
static unique_ptr<GlobalTableFunctionState> TeeInitGlobal(ClientContext &context,
                                                          TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<TeeBindData>();
	return make_uniq<TeeGlobalState>(context, bind_data.types, bind_data.names);
}

static unique_ptr<GlobalTableFunctionState> TeeInitGlobalS(ClientContext &context,
														  TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<TeeBindDataS>();
	return make_uniq<TeeGlobalState>(context, bind_data.types, bind_data.names);
}

static unique_ptr<GlobalTableFunctionState> TeeInitGlobalC(ClientContext &context,
														  TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<TeeBindDataC>();
	return make_uniq<TeeGlobalState>(context, bind_data.types, bind_data.names);
}

// runs when a query runs, decides the schema of the table function output
static unique_ptr<FunctionData> TeeBind(ClientContext &context,
                                        TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types,
                                        vector<string> &names) {
	names = input.input_table_names;
	return_types = input.input_table_types;

	// returns a bind data object
	return make_uniq<TeeBindData>(names, return_types);
}

static unique_ptr<FunctionData> TeeBindS(ClientContext &context,
										TableFunctionBindInput &input,
										vector<LogicalType> &return_types,
										vector<string> &names) {
	names = input.input_table_names;
	return_types = input.input_table_types;

	static idx_t counter = 0;

	string symbol;

	if (input.inputs.size() > 2) {
		throw BinderException("s_tee expects only one string argument.");
	}

	if (input.inputs.size() == 2) {
		symbol = input.inputs[1].GetValue<string>();
	} else {
		++counter;
		symbol = "No " + std::to_string(counter) + ".";
	}

	// returns a bind data object
	return make_uniq<TeeBindDataS>(names, return_types, symbol);
}

static unique_ptr<FunctionData> TeeBindC(ClientContext &context,
										TableFunctionBindInput &input,
										vector<LogicalType> &return_types,
										vector<string> &names) {

	names = input.input_table_names;
	return_types = input.input_table_types;
	string path;


	// FIXME
	if (input.inputs.size() > 2) {
		throw BinderException("c_tee expects only one string argument.");
	}
	if (input.inputs.size() == 2) {
		path = input.inputs[1].GetValue<string>();
	}

	// returns a bind data object
	return make_uniq<TeeBindDataC>(names, return_types, path, context);
}



// called when the extension is loaded
// registers the tee table function and the parser extension
static void LoadInternal(ExtensionLoader &loader) {

	TableFunction tee_function("tee", {LogicalType::TABLE}, nullptr, TeeBind);
	tee_function.init_global = TeeInitGlobal;
	tee_function.in_out_function = TeeTableFun;
	tee_function.in_out_function_final = TeeFinalize;
	loader.RegisterFunction(tee_function);

	TableFunction tee_symbol("s_tee", {LogicalType::TABLE}, nullptr, TeeBindS);
	// We want a default string argument, DuckDB doesn't allow overloading when having TABLE parameter:
	// INTERNAL Error:
	// Function "s_tee" has a TABLE parameter, and multiple function overloads - this is not supported

	// Workaround:
	// Use varargs, allowing us to have 0 to 1 string arguments. If we have more than one, we throw
	// an exception in binder phase
	tee_symbol.varargs = LogicalType::VARCHAR;
	tee_symbol.init_global = TeeInitGlobalS;
	tee_symbol.in_out_function = TeeTableFun;
	tee_symbol.in_out_function_final = TeeFinalizeS;
	loader.RegisterFunction(tee_symbol);

	TableFunction tee_csv("c_tee", {LogicalType::TABLE}, nullptr, TeeBindC);
	tee_csv.varargs = LogicalType::VARCHAR;
	tee_csv.init_global = TeeInitGlobalC;
	tee_csv.in_out_function = TeeTableFun;
	tee_csv.in_out_function_final = TeeFinalizeC;
	loader.RegisterFunction(tee_csv);


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