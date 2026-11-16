
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/function_metadata.hpp"

namespace duckdb {
namespace rest_api_objects {

class LoadFunctionResult {
public:
	LoadFunctionResult();
	LoadFunctionResult(const LoadFunctionResult &) = delete;
	LoadFunctionResult &operator=(const LoadFunctionResult &) = delete;
	LoadFunctionResult(LoadFunctionResult &&) = default;
	LoadFunctionResult &operator=(LoadFunctionResult &&) = default;

public:
	// Deserialization
	static LoadFunctionResult FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	LoadFunctionResult Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	FunctionMetadata metadata;
	optional<string> metadata_location;
};

} // namespace rest_api_objects
} // namespace duckdb
