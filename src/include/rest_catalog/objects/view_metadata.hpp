
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/schema.hpp"
#include "rest_catalog/objects/view_history_entry.hpp"
#include "rest_catalog/objects/view_version.hpp"

using namespace duckdb_yyjson;

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
	static ViewMetadata FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string view_uuid;
	int64_t format_version;
	string location;
	int64_t current_version_id;
	vector<ViewVersion> versions;
	vector<ViewHistoryEntry> version_log;
	vector<Schema> schemas;
	case_insensitive_map_t<string> properties;
	bool has_properties = false;
};

} // namespace rest_api_objects
} // namespace duckdb
