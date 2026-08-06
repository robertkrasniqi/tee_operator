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

class TeeLocalState;

class TeeGlobalState : public ClientContextState {
public:
	TeeGlobalState(ClientContext &context, const TeeOptions &options, const vector<string> &names,
	               const vector<LogicalType> &types, string key);

	void WriteChunk(ClientContext &context, DataChunk &chunk, TeeLocalState &l_state);
	void Flush();
	// Called by the ClientContext once the query is done
	void QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) override;

	void AppendLocalToGlobalBuffer(ColumnDataCollection &local_buffer) {
		lock_guard<mutex> guard(buffer_lock);
		buffered->Combine(local_buffer);
	}

	// only set when we buffer, read by OperatorFinalize
	unique_ptr<ColumnDataCollection> buffered;

private:
	mutex buffer_lock;
	unique_ptr<CSVWriter> csv_writer;
	unique_ptr<Connection> con;
	unique_ptr<Appender> appender;
	mutex appender_lock;
	// key we need to unregister the state in QueryEnd
	string key;

	void TeeInitializeCSVWriter(ClientContext &context, const TeeOptions &options, const vector<string> &names);
	void TeeInitializeTableWriter(ClientContext &context, const TeeOptions &options, const vector<string> &names,
	                              const vector<LogicalType> &types);
};

//!! State of a single thread
class TeeLocalState : public OperatorState {
public:
	TeeLocalState(ClientContext &context, const TeeOptions &options, const vector<LogicalType> &tee_types,
	              shared_ptr<TeeGlobalState> global_state);

	shared_ptr<TeeGlobalState> global_state;
	unique_ptr<ColumnDataCollection> local_buffer;
	ColumnDataAppendState local_append_state;
	unique_ptr<CSVWriterState> local_csv_state;
	DataChunk varchar_chunk_csv;

	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override;

	// Reusing states in recursive CTEs
	bool SupportsReuse() const override {
		return true;
	}

	void Reset() override;
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