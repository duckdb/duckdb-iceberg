
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/namespace.hpp"

namespace duckdb {
namespace rest_api_objects {

class TableIdentifier {
public:
	TableIdentifier();
	TableIdentifier(const TableIdentifier &) = delete;
	TableIdentifier &operator=(const TableIdentifier &) = delete;
	TableIdentifier(TableIdentifier &&) = default;
	TableIdentifier &operator=(TableIdentifier &&) = default;

public:
	// Deserialization
	static TableIdentifier FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	TableIdentifier Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	Namespace _namespace;
	string name;
};

} // namespace rest_api_objects
} // namespace duckdb
