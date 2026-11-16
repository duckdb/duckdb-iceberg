
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class CatalogConfig {
public:
	CatalogConfig();
	CatalogConfig(const CatalogConfig &) = delete;
	CatalogConfig &operator=(const CatalogConfig &) = delete;
	CatalogConfig(CatalogConfig &&) = default;
	CatalogConfig &operator=(CatalogConfig &&) = default;

public:
	// Deserialization
	static CatalogConfig FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	CatalogConfig Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	unordered_map<string, string> defaults;
	unordered_map<string, string> overrides;
	optional<vector<string>> endpoints;
	optional<string> idempotency_key_lifetime;
};

} // namespace rest_api_objects
} // namespace duckdb
