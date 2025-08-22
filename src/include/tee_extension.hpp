#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/box_renderer.hpp"

namespace duckdb {

struct TeeBindData : public FunctionData {

	explicit TeeBindData(vector<string> names) : names(std::move(names)) {}

	vector<string> names;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<TeeBindData>(names);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<TeeBindData>();
		return names == other.names;
	}
};


class TeeExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
