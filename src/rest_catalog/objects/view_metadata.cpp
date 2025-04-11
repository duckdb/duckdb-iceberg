
#include "rest_catalog/objects/view_metadata.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ViewMetadata::ViewMetadata() {
}

ViewMetadata ViewMetadata::FromJSON(yyjson_val *obj) {
	ViewMetadata res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string ViewMetadata::TryFromJSON(yyjson_val *obj) {
	string error;
	auto view_uuid_val = yyjson_obj_get(obj, "view-uuid");
	if (!view_uuid_val) {
		return "ViewMetadata required property 'view-uuid' is missing";
	} else {
		view_uuid = yyjson_get_str(view_uuid_val);
	}
	auto format_version_val = yyjson_obj_get(obj, "format-version");
	if (!format_version_val) {
		return "ViewMetadata required property 'format-version' is missing";
	} else {
		format_version = yyjson_get_sint(format_version_val);
	}
	auto location_val = yyjson_obj_get(obj, "location");
	if (!location_val) {
		return "ViewMetadata required property 'location' is missing";
	} else {
		location = yyjson_get_str(location_val);
	}
	auto current_version_id_val = yyjson_obj_get(obj, "current-version-id");
	if (!current_version_id_val) {
		return "ViewMetadata required property 'current-version-id' is missing";
	} else {
		current_version_id = yyjson_get_sint(current_version_id_val);
	}
	auto versions_val = yyjson_obj_get(obj, "versions");
	if (!versions_val) {
		return "ViewMetadata required property 'versions' is missing";
	} else {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(versions_val, idx, max, val) {
			ViewVersion tmp;
			error = tmp.TryFromJSON(val);
			if (!error.empty()) {
				return error;
			}
			versions.emplace_back(std::move(tmp));
		}
	}
	auto version_log_val = yyjson_obj_get(obj, "version-log");
	if (!version_log_val) {
		return "ViewMetadata required property 'version-log' is missing";
	} else {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(version_log_val, idx, max, val) {
			ViewHistoryEntry tmp;
			error = tmp.TryFromJSON(val);
			if (!error.empty()) {
				return error;
			}
			version_log.emplace_back(std::move(tmp));
		}
	}
	auto schemas_val = yyjson_obj_get(obj, "schemas");
	if (!schemas_val) {
		return "ViewMetadata required property 'schemas' is missing";
	} else {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(schemas_val, idx, max, val) {
			Schema tmp;
			error = tmp.TryFromJSON(val);
			if (!error.empty()) {
				return error;
			}
			schemas.emplace_back(std::move(tmp));
		}
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		has_properties = true;
		properties = parse_object_of_strings(properties_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
