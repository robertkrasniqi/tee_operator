#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator_states.hpp"

namespace duckdb {

// the named parameters of a tee call
struct TeeOptions {
	TeeOptions() = default;
	explicit TeeOptions(const named_parameter_map_t &params) {
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
		if (params.find("maxrows") != params.end()) {
			auto rows = params.at("maxrows").GetValue<int64_t>();
			if (rows < 0) {
				throw InvalidInputException("Tee: maxrows cannot be negative, got maxrows = %d", rows);
			}
			// 0 means render everything
			if (rows == 0) {
				max_rows = NumericLimits<idx_t>::Maximum();
			} else {
				max_rows = static_cast<idx_t>(rows);
			}
		}
	}

	bool NeedsBuffer() const {
		return terminal_flag || pager_flag;
	}

	bool NeedsStream() const {
		return path_flag || table_name_flag;
	}

	// named parameters
	bool pager_flag = false;
	bool terminal_flag = true;
	bool symbol_flag = false;
	string symbol;
	bool path_flag = false;
	string path;
	bool table_name_flag = false;
	string table_name;
	// same default as DuckDB
	idx_t max_rows = 40;
};

class TeeGlobalState : public GlobalOperatorState {
public:
	TeeGlobalState(ClientContext &context, const vector<LogicalType> &types, const vector<string> &names,
	               idx_t all_col_count, const TeeOptions &options)
	    : buffered(context, vector<LogicalType>(types.begin(), types.begin() + all_col_count)), names(names),
	      all_col_count(all_col_count), options(options) {
	}

	ColumnDataCollection buffered;
	vector<string> names;
	idx_t all_col_count;
	mutex lock;
	// owned by the physical operator
	const TeeOptions &options;
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