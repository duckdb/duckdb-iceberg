
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/function_metadata.hpp"

using namespace duckdb_yyjson;

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
	static LoadFunctionResult FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	LoadFunctionResult Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	FunctionMetadata metadata;
	optional<string> metadata_location;
};

} // namespace rest_api_objects
} // namespace duckdb
