
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class SQLViewRepresentation {
public:
	SQLViewRepresentation();
	SQLViewRepresentation(const SQLViewRepresentation &) = delete;
	SQLViewRepresentation &operator=(const SQLViewRepresentation &) = delete;
	SQLViewRepresentation(SQLViewRepresentation &&) = default;
	SQLViewRepresentation &operator=(SQLViewRepresentation &&) = default;

public:
	// Deserialization
	static SQLViewRepresentation FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	SQLViewRepresentation Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	string sql;
	string dialect;
};

} // namespace rest_api_objects
} // namespace duckdb
