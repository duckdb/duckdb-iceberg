
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/function_struct_field.hpp"

namespace duckdb {
namespace rest_api_objects {

class FunctionStructType {
public:
	FunctionStructType();
	FunctionStructType(const FunctionStructType &) = delete;
	FunctionStructType &operator=(const FunctionStructType &) = delete;
	FunctionStructType(FunctionStructType &&) = default;
	FunctionStructType &operator=(FunctionStructType &&) = default;

public:
	// Deserialization
	static FunctionStructType FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FunctionStructType Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	vector<FunctionStructField> fields;
};

} // namespace rest_api_objects
} // namespace duckdb
