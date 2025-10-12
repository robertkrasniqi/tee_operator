#define DUCKDB_EXTENSION_MAIN

#include "tee_extension.hpp"
#include "duckdb/parser/parser_extension.hpp"


namespace duckdb {


// want to make tee(select * ..) possible
// dummy implementation of parser // set up entry
// maybe moving all of this to another file
struct TeeParseData : public ParserExtensionParseData {
	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq<TeeParseData>();
	}
	string ToString() const override {
		return "empty";
	}
};

// dummy parser info
struct TeeParserInfo : public ParserExtensionInfo {
};

class TeeParserExtension {
public:
	static ParserExtensionParseResult ParseFunction(ParserExtensionInfo *info, const string &query) {
		// implement parse logic somewhere here I think
		std::cout << "Debug: Should land here when we try tea(.." << "\n";
		return ParserExtensionParseResult();
	}

	static ParserExtensionPlanResult PlanFunction(ParserExtensionInfo *info, ClientContext &context,
										  unique_ptr<ParserExtensionParseData> parse_data) {
		ParserExtensionPlanResult result;
		// no plan by now
		return result;
	}
};

// load parser
static void LoadParserExtension(DuckDB &db) {
	ParserExtension tee_parser;

	tee_parser.parser_info = make_shared_ptr<TeeParserInfo>();
	tee_parser.parse_function = TeeParserExtension::ParseFunction;
	tee_parser.plan_function = TeeParserExtension::PlanFunction;

	db.instance->config.parser_extensions.push_back(tee_parser);
}

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
		std::cout << "Tee Operator" << std::endl;

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


// called when the extension is loaded
// registers the tee table function
static void LoadInternal(DatabaseInstance &instance) {

	auto tee_function = TableFunction("tee", {LogicalType::TABLE}, nullptr, TeeBind);

	tee_function.init_global = TeeInitGlobal; // global state init
	tee_function.in_out_function = TeeTableFun; // per chunk
	tee_function.in_out_function_final = TeeFinalize; // end

	ExtensionUtil::RegisterFunction(instance, tee_function);
}

void TeeExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
	// parser entry
	LoadParserExtension(db);
}

std::string TeeExtension::Name() {
	return "tee";
}

std::string TeeExtension::Version() const {
#ifdef EXT_VERSION_TEE
	return EXT_VERSION_TEE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void tee_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	// look in autocomplete extension how they solved this
	db_wrapper.LoadExtension<duckdb::TeeExtension>();
}

DUCKDB_EXTENSION_API const char *tee_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
