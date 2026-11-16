
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class StructField;

class StructType {
public:
	StructType();
	StructType(const StructType &) = delete;
	StructType &operator=(const StructType &) = delete;
	StructType(StructType &&) = default;
	StructType &operator=(StructType &&) = default;

public:
	// Deserialization
	static StructType FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	StructType Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	vector<unique_ptr<StructField>> fields;
};

} // namespace rest_api_objects
} // namespace duckdb
