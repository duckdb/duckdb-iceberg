
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class CatalogObjectIdentifier {
public:
	CatalogObjectIdentifier();
	CatalogObjectIdentifier(const CatalogObjectIdentifier &) = delete;
	CatalogObjectIdentifier &operator=(const CatalogObjectIdentifier &) = delete;
	CatalogObjectIdentifier(CatalogObjectIdentifier &&) = default;
	CatalogObjectIdentifier &operator=(CatalogObjectIdentifier &&) = default;

public:
	// Deserialization
	static CatalogObjectIdentifier FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	CatalogObjectIdentifier Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	vector<string> value;
};

} // namespace rest_api_objects
} // namespace duckdb
