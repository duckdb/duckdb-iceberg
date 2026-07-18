
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/function_representation.hpp"

using namespace duckdb_yyjson;

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
	static FunctionDefinitionVersion FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	FunctionDefinitionVersion Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	int32_t version_id;
	vector<FunctionRepresentation> representations;
	int64_t timestamp_ms;
	optional<bool> deterministic;
	optional<string> on_null_input;
};

} // namespace rest_api_objects
} // namespace duckdb
