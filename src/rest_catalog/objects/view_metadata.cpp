
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
		if (yyjson_is_str(view_uuid_val)) {
			view_uuid = yyjson_get_str(view_uuid_val);
		} else {
			return StringUtil::Format("ViewMetadata property 'view_uuid' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(view_uuid_val));
		}
	}
	auto format_version_val = yyjson_obj_get(obj, "format-version");
	if (!format_version_val) {
		return "ViewMetadata required property 'format-version' is missing";
	} else {
		if (yyjson_is_int(format_version_val)) {
			format_version = yyjson_get_int(format_version_val);
		} else {
			return StringUtil::Format(
			    "ViewMetadata property 'format_version' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(format_version_val));
		}
	}
	auto location_val = yyjson_obj_get(obj, "location");
	if (!location_val) {
		return "ViewMetadata required property 'location' is missing";
	} else {
		if (yyjson_is_str(location_val)) {
			location = yyjson_get_str(location_val);
		} else {
			return StringUtil::Format("ViewMetadata property 'location' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(location_val));
		}
	}
	auto current_version_id_val = yyjson_obj_get(obj, "current-version-id");
	if (!current_version_id_val) {
		return "ViewMetadata required property 'current-version-id' is missing";
	} else {
		if (yyjson_is_int(current_version_id_val)) {
			current_version_id = yyjson_get_int(current_version_id_val);
		} else {
			return StringUtil::Format(
			    "ViewMetadata property 'current_version_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(current_version_id_val));
		}
	}
	auto versions_val = yyjson_obj_get(obj, "versions");
	if (!versions_val) {
		return "ViewMetadata required property 'versions' is missing";
	} else {
		if (yyjson_is_arr(versions_val)) {
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
		} else {
			return StringUtil::Format("ViewMetadata property 'versions' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(versions_val));
		}
	}
	auto version_log_val = yyjson_obj_get(obj, "version-log");
	if (!version_log_val) {
		return "ViewMetadata required property 'version-log' is missing";
	} else {
		if (yyjson_is_arr(version_log_val)) {
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
		} else {
			return StringUtil::Format("ViewMetadata property 'version_log' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(version_log_val));
		}
	}
	auto schemas_val = yyjson_obj_get(obj, "schemas");
	if (!schemas_val) {
		return "ViewMetadata required property 'schemas' is missing";
	} else {
		if (yyjson_is_arr(schemas_val)) {
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
		} else {
			return StringUtil::Format("ViewMetadata property 'schemas' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(schemas_val));
		}
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		has_properties = true;
		if (yyjson_is_obj(properties_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(properties_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format("ViewMetadata property 'tmp' is not of type 'string', found '%s' instead",
					                          yyjson_get_type_desc(val));
				}
				properties.emplace(key_str, std::move(tmp));
			}
		} else {
			return "ViewMetadata property 'properties' is not of type 'object'";
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
