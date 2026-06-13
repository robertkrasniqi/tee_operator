#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator_states.hpp"

namespace duckdb {

class TeeGlobalState : public GlobalOperatorState {
public:
	TeeGlobalState(ClientContext &context, const vector<LogicalType> &types, const vector<string> &names,
	               idx_t all_col_count)
	    : buffered(context, vector<LogicalType>(types.begin(), types.begin() + all_col_count)), names(names),
	      all_col_count(all_col_count) {
	}

	ColumnDataCollection buffered;
	vector<string> names;
	idx_t all_col_count;
	mutex lock;
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