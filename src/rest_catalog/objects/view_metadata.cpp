
#include "rest_catalog/objects/view_metadata.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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

ViewMetadata ViewMetadata::Copy() const {
	ViewMetadata res;
	res.view_uuid = view_uuid;
	res.format_version = format_version;
	res.location = location;
	res.current_version_id = current_version_id;
	res.versions.reserve(versions.size());
	for (auto &item : versions) {
		res.versions.emplace_back(item.Copy());
	}
	res.version_log.reserve(version_log.size());
	for (auto &item : version_log) {
		res.version_log.emplace_back(item.Copy());
	}
	res.schemas.reserve(schemas.size());
	for (auto &item : schemas) {
		res.schemas.emplace_back(item.Copy());
	}
	if (properties.has_value()) {
		res.properties.emplace();
		for (auto &entry : (*properties)) {
			(*res.properties).emplace(entry.first, entry.second);
		}
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
			size_t versions_idx, versions_max;
			yyjson_val *versions_item_val;
			yyjson_arr_foreach(versions_val, versions_idx, versions_max, versions_item_val) {
				ViewVersion versions_item;
				error = versions_item.TryFromJSON(versions_item_val);
				if (!error.empty()) {
					return error;
				}
				versions.emplace_back(std::move(versions_item));
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
			size_t version_log_idx, version_log_max;
			yyjson_val *version_log_item_val;
			yyjson_arr_foreach(version_log_val, version_log_idx, version_log_max, version_log_item_val) {
				ViewHistoryEntry version_log_item;
				error = version_log_item.TryFromJSON(version_log_item_val);
				if (!error.empty()) {
					return error;
				}
				version_log.emplace_back(std::move(version_log_item));
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
			size_t schemas_idx, schemas_max;
			yyjson_val *schemas_item_val;
			yyjson_arr_foreach(schemas_val, schemas_idx, schemas_max, schemas_item_val) {
				Schema schemas_item;
				error = schemas_item.TryFromJSON(schemas_item_val);
				if (!error.empty()) {
					return error;
				}
				schemas.emplace_back(std::move(schemas_item));
			}
		} else {
			return StringUtil::Format("ViewMetadata property 'schemas' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(schemas_val));
		}
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		case_insensitive_map_t<string> properties_tmp;
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
				properties_tmp.emplace(key_str, std::move(tmp));
			}
		} else {
			return "ViewMetadata property 'properties_tmp' is not of type 'object'";
		}
		properties = std::move(properties_tmp);
	}
	return "";
}

void ViewMetadata::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: view-uuid
	yyjson_mut_obj_add_strcpy(doc, obj, "view-uuid", view_uuid.c_str());

	// Serialize: format-version
	yyjson_mut_obj_add_int(doc, obj, "format-version", format_version);

	// Serialize: location
	yyjson_mut_obj_add_strcpy(doc, obj, "location", location.c_str());

	// Serialize: current-version-id
	yyjson_mut_obj_add_int(doc, obj, "current-version-id", current_version_id);

	// Serialize: versions
	yyjson_mut_val *versions_arr = yyjson_mut_arr(doc);
	for (const auto &item : versions) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(versions_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "versions", versions_arr);

	// Serialize: version-log
	yyjson_mut_val *version_log_arr = yyjson_mut_arr(doc);
	for (const auto &item : version_log) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(version_log_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "version-log", version_log_arr);

	// Serialize: schemas
	yyjson_mut_val *schemas_arr = yyjson_mut_arr(doc);
	for (const auto &item : schemas) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(schemas_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "schemas", schemas_arr);

	// Serialize: properties
	if (properties.has_value()) {
		auto &properties_value = *properties;
		yyjson_mut_val *properties_value_obj = yyjson_mut_obj(doc);
		for (const auto &it : properties_value) {
			auto &key = it.first;
			auto &value = it.second;
			auto key_ptr = unsafe_yyjson_mut_strncpy(doc, key.c_str(), strlen(key.c_str()));
			yyjson_mut_obj_add_strcpy(doc, properties_value_obj, key_ptr, value.c_str());
		}
		yyjson_mut_obj_add_val(doc, obj, "properties", properties_value_obj);
	}
}

yyjson_mut_val *ViewMetadata::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
