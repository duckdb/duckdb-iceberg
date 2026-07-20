
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/function_definition_version_ref.hpp"

using namespace duckdb_yyjson;

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
	static FunctionDefinitionLogEntry FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	FunctionDefinitionLogEntry Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	int64_t timestamp_ms;
	vector<FunctionDefinitionVersionRef> definition_versions;
};

} // namespace rest_api_objects
} // namespace duckdb
