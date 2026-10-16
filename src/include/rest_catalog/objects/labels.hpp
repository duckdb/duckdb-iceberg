
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/catalog_object_labels.hpp"
#include "rest_catalog/objects/field_labels.hpp"

namespace duckdb {
namespace rest_api_objects {

class Labels {
public:
	Labels();
	Labels(const Labels &) = delete;
	Labels &operator=(const Labels &) = delete;
	Labels(Labels &&) = default;
	Labels &operator=(Labels &&) = default;

public:
	// Deserialization
	static Labels FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	Labels Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<CatalogObjectLabels> object_labels;
	optional<vector<FieldLabels>> fields;
};

} // namespace rest_api_objects
} // namespace duckdb
