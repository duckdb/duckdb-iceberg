
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class CatalogObjectLabels {
public:
	CatalogObjectLabels();
	CatalogObjectLabels(const CatalogObjectLabels &) = delete;
	CatalogObjectLabels &operator=(const CatalogObjectLabels &) = delete;
	CatalogObjectLabels(CatalogObjectLabels &&) = default;
	CatalogObjectLabels &operator=(CatalogObjectLabels &&) = default;

public:
	// Deserialization
	static CatalogObjectLabels FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	CatalogObjectLabels Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	case_insensitive_map_t<string> additional_properties;
};

} // namespace rest_api_objects
} // namespace duckdb
