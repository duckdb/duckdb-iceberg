
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class FunctionDataType;

class FunctionMapType {
public:
	FunctionMapType();
	FunctionMapType(const FunctionMapType &) = delete;
	FunctionMapType &operator=(const FunctionMapType &) = delete;
	FunctionMapType(FunctionMapType &&) = default;
	FunctionMapType &operator=(FunctionMapType &&) = default;

public:
	// Deserialization
	static FunctionMapType FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FunctionMapType Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	unique_ptr<FunctionDataType> key;
	unique_ptr<FunctionDataType> value;
};

} // namespace rest_api_objects
} // namespace duckdb
