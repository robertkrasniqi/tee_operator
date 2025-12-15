#pragma once

#include "duckdb.hpp"
#include "tee_parser.hpp"
#include "duckdb/common/box_renderer.hpp"

namespace duckdb {

struct TeeBindData : public FunctionData {
	TeeBindData(vector<string> names_p, vector<LogicalType> types_p, named_parameter_map_t tee_named_parameter_p)
	    : names(std::move(names_p)), types(std::move(types_p)), tee_named_parameters(std::move(tee_named_parameter_p)) {
	}

	vector<string> names;
	vector<LogicalType> types;
	named_parameter_map_t tee_named_parameters;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<TeeBindData>(names, types, tee_named_parameters);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<TeeBindData>();
		return names == other.names && types == other.types && tee_named_parameters == other.tee_named_parameters;
	}
};

struct TeeGlobalState : public GlobalTableFunctionState {
	TeeGlobalState(ClientContext &context, const vector<LogicalType> &types_p, const vector<string> &names_p)
	    : buffered(context, types_p), names(names_p), printed(false) {
	}
	ColumnDataCollection buffered;
	vector<string> names;
	mutex lock;
	// flag to ensure we only print once at end
	bool printed;
};


class TeeExtension : public Extension {
public:
	// load() is called when the extension is installed into a database
	void Load(ExtensionLoader &loader) override;

	std::string Name() override {
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
