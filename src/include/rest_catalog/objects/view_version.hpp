
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/namespace.hpp"
#include "rest_catalog/objects/view_representation.hpp"

namespace duckdb {
namespace rest_api_objects {

class ViewVersion {
public:
	ViewVersion();
	ViewVersion(const ViewVersion &) = delete;
	ViewVersion &operator=(const ViewVersion &) = delete;
	ViewVersion(ViewVersion &&) = default;
	ViewVersion &operator=(ViewVersion &&) = default;

public:
	// Deserialization
	static ViewVersion FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ViewVersion Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	int32_t version_id;
	int64_t timestamp_ms;
	int32_t schema_id;
	unordered_map<string, string> summary;
	vector<ViewRepresentation> representations;
	Namespace default_namespace;
	optional<string> default_catalog;
};

} // namespace rest_api_objects
} // namespace duckdb
