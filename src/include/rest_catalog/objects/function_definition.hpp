
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/function_definition_version.hpp"
#include "rest_catalog/objects/function_parameter.hpp"

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
	static FunctionDefinition FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FunctionDefinition Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string definition_id;
	vector<FunctionParameter> parameters;
	unique_ptr<FunctionDataType> return_type;
	vector<FunctionDefinitionVersion> versions;
	int32_t current_version_id;
	string function_type;
	optional<string> specific_name;
	optional<bool> return_nullable;
	optional<string> _doc;
};

} // namespace rest_api_objects
} // namespace duckdb
