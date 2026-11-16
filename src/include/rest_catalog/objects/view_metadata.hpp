
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/schema.hpp"
#include "rest_catalog/objects/view_history_entry.hpp"
#include "rest_catalog/objects/view_version.hpp"

namespace duckdb {
namespace rest_api_objects {

class ViewMetadata {
public:
	ViewMetadata();
	ViewMetadata(const ViewMetadata &) = delete;
	ViewMetadata &operator=(const ViewMetadata &) = delete;
	ViewMetadata(ViewMetadata &&) = default;
	ViewMetadata &operator=(ViewMetadata &&) = default;

public:
	// Deserialization
	static ViewMetadata FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ViewMetadata Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string view_uuid;
	int32_t format_version;
	string location;
	int32_t current_version_id;
	vector<ViewVersion> versions;
	vector<ViewHistoryEntry> version_log;
	vector<Schema> schemas;
	optional<unordered_map<string, string>> properties;
};

} // namespace rest_api_objects
} // namespace duckdb
