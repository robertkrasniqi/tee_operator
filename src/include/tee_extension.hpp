#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator_states.hpp"

namespace duckdb {

class TeeGlobalState : public GlobalOperatorState {
public:
	TeeGlobalState(ClientContext &context, const vector<LogicalType> &types, const vector<string> &names,
	               idx_t all_col_count, const named_parameter_map_t &params)
	    : buffered(context, vector<LogicalType>(types.begin(), types.begin() + all_col_count)), names(names),
	      all_col_count(all_col_count), pager_flag(false), terminal_flag(true), symbol_flag(false), symbol(""),
	      path_flag(false), path(""), table_name_flag(false), table_name("") {
		
		if (params.find("pager") != params.end()) {
			pager_flag = params.at("pager").GetValue<bool>();
		}
		if (params.find("terminal") != params.end()) {
			terminal_flag = params.at("terminal").GetValue<bool>();
		}
		if (params.find("symbol") != params.end()) {
			symbol_flag = true;
			symbol = params.at("symbol").GetValue<string>();
		}
		if (params.find("path") != params.end()) {
			path_flag = true;
			path = params.at("path").GetValue<string>();
		}
		if (params.find("table_name") != params.end()) {
			table_name_flag = true;
			table_name = params.at("table_name").GetValue<string>();
		}
	}

	ColumnDataCollection buffered;
	vector<string> names;
	idx_t all_col_count;
	mutex lock;

	// named parameters
	bool pager_flag;
	bool terminal_flag;
	bool symbol_flag;
	string symbol;
	bool path_flag;
	string path;
	bool table_name_flag;
	string table_name;
};

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

class TeeExtension : public Extension {
public:
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