#pragma once

#include "duckdb.hpp"
#include "tee_parser.hpp"
#include "duckdb/common/box_renderer.hpp"

namespace duckdb {

struct TeeBindData : public FunctionData {
	TeeBindData(vector<string> names_p, vector<LogicalType> types_p)
	    : names(std::move(names_p)), types(std::move(types_p)) {}

	vector<string> names;
	vector<LogicalType> types;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<TeeBindData>(names, types);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<TeeBindData>();
		return names == other.names && types == other.types;
	}
};

struct TeeBindDataS : public FunctionData {
	TeeBindDataS(vector<string> names_p, vector<LogicalType> types_p, string symbol_p)
		: names(std::move(names_p)), types(std::move(types_p)), symbol(std::move(symbol_p)) {}

	vector<string> names;
	vector<LogicalType> types;
	string symbol;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<TeeBindDataS>(names, types, symbol);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<TeeBindDataS>();
		return names == other.names && types == other.types && symbol == other.symbol;
	}
};

struct TeeBindDataC : public FunctionData {
	TeeBindDataC(vector<string> names_p, vector<LogicalType> types_p, string path_p, ClientContext &context_p)
	    : names(std::move(names_p)), types(std::move(types_p)), path(path_p), context(context_p) {
	}

	// maybe delete context_p later?
	// need it for FileSystem &fs =  FileSystem::GetFileSystem(context.client);	in FinalizeC
	vector<string> names;
	vector<LogicalType> types;
	string path;
	ClientContext &context;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<TeeBindDataC>(names, types, path, context);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<TeeBindDataC>();
		return names == other.names && types == other.types && other.path == path;
	}
};


struct TeeGlobalState : public GlobalTableFunctionState {
	TeeGlobalState(ClientContext &context,
	               const vector<LogicalType> &types_p,
	               const vector<string> &names_p)
	    : buffered(context, types_p), names(names_p), printed(false) {
	}

	ColumnDataCollection buffered;
	vector<string> names;
	// flag to ensure we only print once at end
	bool printed;
};

class TeeExtension : public Extension {
public:
	// load() is called when the extension is installed into a database
	void Load(ExtensionLoader &loader) override;

	std::string Name() override{
		return "tee";
	}

	std::string Version() const override {
	#ifdef EXT_VERSION_TEE
		return EXT_VERSION_TEE;
	#else
		return "";
	#endif
	}
};

} // namespace duckdb
