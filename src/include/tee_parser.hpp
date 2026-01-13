#pragma once

#include "tee_operator.hpp"
#include "tee_extension.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {
class TeeState : public ClientContextState {
public:
	explicit TeeState(unique_ptr<ParserExtensionParseData> parse_data) : parse_data(std::move(parse_data)) {
	}

	void QueryEnd() override {
		parse_data.reset();
	}

	unique_ptr<ParserExtensionParseData> parse_data;
};

struct TeeParseData : ParserExtensionParseData {
	unique_ptr<SQLStatement> statement;
	unordered_map<string, string> options;

	TeeParseData(unique_ptr<SQLStatement> statement, unordered_map<string, string> options)
	    : statement(std::move(statement)), options(std::move(options)) {
	}

	string ToString() const override {
		return "TeeData";
	}

	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq_base<ParserExtensionParseData, TeeParseData>(statement->Copy(), options);
	}
};

ParserExtensionParseResult TeeParse(ParserExtensionInfo *info, const std::string &query);

ParserExtensionPlanResult TeePlan(ParserExtensionInfo *info, ClientContext &context,
                                  unique_ptr<ParserExtensionParseData> parse_data);

struct TeeParserExtension : public ParserExtension {
	TeeParserExtension() : ParserExtension() {
		parse_function = TeeParse;
		plan_function = TeePlan;
	}
};

BoundStatement TeeBindParser(ClientContext &context, Binder &binder, OperatorExtensionInfo *info,
                             SQLStatement &statement);

struct TeeExtensionParser : public OperatorExtension {
	TeeExtensionParser() : OperatorExtension() {
		Bind = TeeBindParser;
	}

	std::string GetName() override {
		return "tee";
	}

	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &deserializer) override {
		throw InternalException("tee operator should not be serialized");
	}
};
} // namespace duckdb
/*
 *
struct TeeParseData : public ParserExtensionParseData {
    unique_ptr<ParserExtensionParseData> Copy() const override {
        return make_uniq<TeeParseData>();
    }
    string ToString() const override {
        return "Parsed Data";
    }
};

struct TeeParserInfo : public ParserExtensionInfo {};

class TeeParserExtension : public ParserExtension {
public:
    static ParserOverrideResult ParserOverrideFunction(ParserExtensionInfo *info, const string &query);
};

}; // namespace duckdb
*/