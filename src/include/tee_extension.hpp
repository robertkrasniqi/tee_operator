#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Holds the named parameters of a Tee call
struct TeeOptions {
	TeeOptions() = default;
	explicit TeeOptions(const named_parameter_map_t &params) {
		if (params.find("pager") != params.end()) {
			has_pager = params.at("pager").GetValue<bool>();
		}
		if (params.find("terminal") != params.end()) {
			has_terminal = params.at("terminal").GetValue<bool>();
		}
		if (params.find("symbol") != params.end()) {
			has_symbol = true;
			symbol = params.at("symbol").GetValue<string>();
		}
		if (params.find("path") != params.end()) {
			has_path = true;
			path = params.at("path").GetValue<string>();
		}
		if (params.find("table_name") != params.end()) {
			has_table = true;
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

	bool has_pager = false;
	bool has_terminal = true;
	bool has_symbol = false;
	string symbol;
	bool has_path = false;
	string path;
	bool has_table = false;
	string table_name;
	// same default as DuckDB
	idx_t max_rows = 40;
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