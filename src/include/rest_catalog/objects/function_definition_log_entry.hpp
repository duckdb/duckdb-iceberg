
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/function_definition_version_ref.hpp"

namespace duckdb {
namespace rest_api_objects {

class FunctionDefinitionLogEntry {
public:
	FunctionDefinitionLogEntry();
	FunctionDefinitionLogEntry(const FunctionDefinitionLogEntry &) = delete;
	FunctionDefinitionLogEntry &operator=(const FunctionDefinitionLogEntry &) = delete;
	FunctionDefinitionLogEntry(FunctionDefinitionLogEntry &&) = default;
	FunctionDefinitionLogEntry &operator=(FunctionDefinitionLogEntry &&) = default;

public:
	// Deserialization
	static FunctionDefinitionLogEntry FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FunctionDefinitionLogEntry Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	int64_t timestamp_ms;
	vector<FunctionDefinitionVersionRef> definition_versions;
};

} // namespace rest_api_objects
} // namespace duckdb
