#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

struct DeserializeResult {
public:
	DeserializeResult(Value &&val) : value(std::move(val)) {
	}
	DeserializeResult(const string &error) : error(error) {
	}

public:
	bool HasError() const {
		return !error.empty();
	}
	const string GetError() const {
		D_ASSERT(HasError());
		return error;
	}
	const Value &GetValue() const {
		D_ASSERT(!HasError());
		return value;
	}

public:
	Value value;
	string error;
};

struct SerializeStats {
public:
	SerializeStats(string &input, LogicalType &column_type) : input(input), original_type(column_type) {
	}
	SerializeStats(const string &error) : error(error) {
	}

public:
	bool HasError() const {
		return !error.empty();
	}
	const string GetError() const {
		D_ASSERT(HasError());
		return error;
	}
	const Value &GetValue() const {
		D_ASSERT(!HasError());
		return value;
	}

public:
	string input;
	string error;
	LogicalType original_type;
	Value value;
};

struct IcebergValue {
public:
	IcebergValue() = delete;

public:
	static DeserializeResult DeserializeValue(const string_t &blob, const LogicalType &target);
	static SerializeStats SerializeValue(string &value, LogicalType &column_type);
};

} // namespace duckdb
