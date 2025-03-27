#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/view_version.hpp"
#include "rest_catalog/objects/schema.hpp"
#include "rest_catalog/objects/view_history_entry.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewMetadata {
public:
	static ViewMetadata FromJSON(yyjson_val *obj) {
		ViewMetadata result;
		auto view_uuid_val = yyjson_obj_get(obj, "view-uuid");
		if (view_uuid_val) {
			result.view_uuid = yyjson_get_str(view_uuid_val);
		}
		auto format_version_val = yyjson_obj_get(obj, "format-version");
		if (format_version_val) {
			result.format_version = yyjson_get_sint(format_version_val);
		}
		auto location_val = yyjson_obj_get(obj, "location");
		if (location_val) {
			result.location = yyjson_get_str(location_val);
		}
		auto current_version_id_val = yyjson_obj_get(obj, "current-version-id");
		if (current_version_id_val) {
			result.current_version_id = yyjson_get_sint(current_version_id_val);
		}
		auto versions_val = yyjson_obj_get(obj, "versions");
		if (versions_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(versions_val, idx, max, val) {
				result.versions.push_back(ViewVersion::FromJSON(val));
			}
		}
		auto version_log_val = yyjson_obj_get(obj, "version-log");
		if (version_log_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(version_log_val, idx, max, val) {
				result.version_log.push_back(ViewHistoryEntry::FromJSON(val));
			}
		}
		auto schemas_val = yyjson_obj_get(obj, "schemas");
		if (schemas_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(schemas_val, idx, max, val) {
				result.schemas.push_back(Schema::FromJSON(val));
			}
		}
		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			result.properties = parse_object_of_strings(properties_val);
		}
		return result;
	}
public:
	string view_uuid;
	int64_t format_version;
	string location;
	int64_t current_version_id;
	vector<ViewVersion> versions;
	vector<ViewHistoryEntry> version_log;
	vector<Schema> schemas;
	ObjectOfStrings properties;
};

} // namespace rest_api_objects
} // namespace duckdb