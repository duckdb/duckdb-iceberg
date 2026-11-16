
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class FunctionDataType;

class FunctionListType {
public:
	FunctionListType();
	FunctionListType(const FunctionListType &) = delete;
	FunctionListType &operator=(const FunctionListType &) = delete;
	FunctionListType(FunctionListType &&) = default;
	FunctionListType &operator=(FunctionListType &&) = default;

public:
	// Deserialization
	static FunctionListType FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FunctionListType Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	unique_ptr<FunctionDataType> element;
};

} // namespace rest_api_objects
} // namespace duckdb
