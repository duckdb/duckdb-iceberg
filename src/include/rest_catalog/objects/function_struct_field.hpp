
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class FunctionDataType;

class FunctionStructField {
public:
	FunctionStructField();
	FunctionStructField(const FunctionStructField &) = delete;
	FunctionStructField &operator=(const FunctionStructField &) = delete;
	FunctionStructField(FunctionStructField &&) = default;
	FunctionStructField &operator=(FunctionStructField &&) = default;

public:
	// Deserialization
	static FunctionStructField FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FunctionStructField Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string name;
	unique_ptr<FunctionDataType> type;
};

} // namespace rest_api_objects
} // namespace duckdb
