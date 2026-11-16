
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class FunctionSQLRepresentation {
public:
	FunctionSQLRepresentation();
	FunctionSQLRepresentation(const FunctionSQLRepresentation &) = delete;
	FunctionSQLRepresentation &operator=(const FunctionSQLRepresentation &) = delete;
	FunctionSQLRepresentation(FunctionSQLRepresentation &&) = default;
	FunctionSQLRepresentation &operator=(FunctionSQLRepresentation &&) = default;

public:
	// Deserialization
	static FunctionSQLRepresentation FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FunctionSQLRepresentation Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	string dialect;
	string sql;
};

} // namespace rest_api_objects
} // namespace duckdb
