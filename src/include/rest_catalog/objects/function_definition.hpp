
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/function_definition_version.hpp"
#include "rest_catalog/objects/function_parameter.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FunctionDataType;

class FunctionDefinition {
public:
	FunctionDefinition();
	FunctionDefinition(const FunctionDefinition &) = delete;
	FunctionDefinition &operator=(const FunctionDefinition &) = delete;
	FunctionDefinition(FunctionDefinition &&) = default;
	FunctionDefinition &operator=(FunctionDefinition &&) = default;

public:
	// Deserialization
	static FunctionDefinition FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	FunctionDefinition Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string definition_id;
	vector<FunctionParameter> parameters;
	unique_ptr<FunctionDataType> return_type;
	vector<FunctionDefinitionVersion> versions;
	int32_t current_version_id;
	string function_type;
	optional<bool> return_nullable;
	optional<string> _doc;
};

} // namespace rest_api_objects
} // namespace duckdb
