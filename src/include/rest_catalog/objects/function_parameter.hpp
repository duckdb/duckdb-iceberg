
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class FunctionDataType;

class FunctionParameter {
public:
	FunctionParameter();
	FunctionParameter(const FunctionParameter &) = delete;
	FunctionParameter &operator=(const FunctionParameter &) = delete;
	FunctionParameter(FunctionParameter &&) = default;
	FunctionParameter &operator=(FunctionParameter &&) = default;

public:
	// Deserialization
	static FunctionParameter FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FunctionParameter Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	unique_ptr<FunctionDataType> type;
	string name;
	optional<string> _doc;
};

} // namespace rest_api_objects
} // namespace duckdb
