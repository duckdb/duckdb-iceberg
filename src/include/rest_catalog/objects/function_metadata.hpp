
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/function_definition.hpp"
#include "rest_catalog/objects/function_definition_log_entry.hpp"

using namespace duckdb_yyjson;

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
	static FunctionMetadata FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	FunctionMetadata Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string function_uuid;
	int32_t format_version;
	vector<FunctionDefinition> definitions;
	vector<FunctionDefinitionLogEntry> definition_log;
	optional<string> location;
	optional<case_insensitive_map_t<string>> properties;
	optional<bool> secure;
	optional<string> _doc;
};

} // namespace rest_api_objects
} // namespace duckdb
