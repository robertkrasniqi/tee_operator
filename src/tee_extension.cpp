#include "tee_extension.hpp"
#include "tee_parser.hpp"
#include <regex>


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

// this function is called once at the start of execution to create the global state
static unique_ptr<GlobalTableFunctionState> TeeInitGlobal(ClientContext &context,
                                                          TableFunctionInitInput &input) {
	// get the bind data
	auto &bind_data = input.bind_data->Cast<TeeBindData>();

	// initialize global state with both types and names
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
	tee_symbol.init_global = TeeInitGlobal;
	tee_symbol.in_out_function = TeeTableFun;
	tee_symbol.in_out_function_final = TeeFinalizeS;
	loader.RegisterFunction(tee_symbol);

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