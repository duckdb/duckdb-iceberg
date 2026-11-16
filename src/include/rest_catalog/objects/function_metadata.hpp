
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/function_definition.hpp"
#include "rest_catalog/objects/function_definition_log_entry.hpp"

namespace duckdb {
namespace rest_api_objects {

class FunctionMetadata {
public:
	FunctionMetadata();
	FunctionMetadata(const FunctionMetadata &) = delete;
	FunctionMetadata &operator=(const FunctionMetadata &) = delete;
	FunctionMetadata(FunctionMetadata &&) = default;
	FunctionMetadata &operator=(FunctionMetadata &&) = default;

public:
	// Deserialization
	static FunctionMetadata FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FunctionMetadata Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string function_uuid;
	int32_t format_version;
	vector<FunctionDefinition> definitions;
	vector<FunctionDefinitionLogEntry> definition_log;
	optional<string> location;
	optional<unordered_map<string, string>> properties;
	optional<bool> secure;
	optional<string> _doc;
};

} // namespace rest_api_objects
} // namespace duckdb
