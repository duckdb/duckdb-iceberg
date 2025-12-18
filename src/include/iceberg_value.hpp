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

struct SerializeResult {
public:
	SerializeResult(LogicalType &column_type, Value serialized_value)
	    : original_type(column_type), value(serialized_value) {
	}

	SerializeResult() : original_type(LogicalType::INVALID), value(Value()) {
	}

	explicit SerializeResult(const string &error) : error(error) {
	}

public:
	bool HasError() const {
		return !error.empty();
	}
	string GetError() const {
		D_ASSERT(HasError());
		return error;
	}
	const Value &GetValue() const {
		D_ASSERT(!HasError());
		D_ASSERT(value.type() == LogicalType::BLOB);
		return value;
	}
	// some returned stats are known to be incorrect. For that we do not serialize them
	bool HasValue() const {
		return !value.IsNull();
	}

public:
	string error;
	LogicalType original_type;
	Value value;
};

struct IcebergValue {
public:
	IcebergValue() = delete;

public:
	static DeserializeResult DeserializeValue(const string_t &blob, const LogicalType &target);
	static SerializeResult SerializeValue(Value input_value, LogicalType &column_type);
};

} // namespace duckdb
