
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/function_representation.hpp"

namespace duckdb {
namespace rest_api_objects {

class FunctionDefinitionVersion {
public:
	FunctionDefinitionVersion();
	FunctionDefinitionVersion(const FunctionDefinitionVersion &) = delete;
	FunctionDefinitionVersion &operator=(const FunctionDefinitionVersion &) = delete;
	FunctionDefinitionVersion(FunctionDefinitionVersion &&) = default;
	FunctionDefinitionVersion &operator=(FunctionDefinitionVersion &&) = default;

public:
	// Deserialization
	static FunctionDefinitionVersion FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FunctionDefinitionVersion Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	int32_t version_id;
	vector<FunctionRepresentation> representations;
	int64_t timestamp_ms;
	optional<bool> deterministic;
	optional<string> on_null_input;
};

} // namespace rest_api_objects
} // namespace duckdb
