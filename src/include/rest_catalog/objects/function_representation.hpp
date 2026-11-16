
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/function_sqlrepresentation.hpp"

namespace duckdb {
namespace rest_api_objects {

class FunctionRepresentation {
public:
	FunctionRepresentation();
	FunctionRepresentation(const FunctionRepresentation &) = delete;
	FunctionRepresentation &operator=(const FunctionRepresentation &) = delete;
	FunctionRepresentation(FunctionRepresentation &&) = default;
	FunctionRepresentation &operator=(FunctionRepresentation &&) = default;

public:
	// Deserialization
	static FunctionRepresentation FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FunctionRepresentation Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<FunctionSQLRepresentation> function_sqlrepresentation;
};

} // namespace rest_api_objects
} // namespace duckdb
