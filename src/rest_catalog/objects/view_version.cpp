
#include "rest_catalog/objects/view_version.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ViewVersion::ViewVersion() {
}

ViewVersion ViewVersion::FromJSON(yyjson_val *obj) {
	ViewVersion res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ViewVersion ViewVersion::Copy() const {
	ViewVersion res;
	res.version_id = version_id;
	res.timestamp_ms = timestamp_ms;
	res.schema_id = schema_id;
	for (auto &entry : summary) {
		res.summary.emplace(entry.first, entry.second);
	}
	res.representations.reserve(representations.size());
	for (auto &item : representations) {
		res.representations.emplace_back(item.Copy());
	}
	res.default_namespace = default_namespace.Copy();
	if (default_catalog.has_value()) {
		res.default_catalog.emplace();
		(*res.default_catalog) = (*default_catalog);
	}
	return res;
}

string ViewVersion::TryFromJSON(yyjson_val *obj) {
	string error;
	auto version_id_val = yyjson_obj_get(obj, "version-id");
	if (!version_id_val) {
		return "ViewVersion required property 'version-id' is missing";
	} else {
		if (yyjson_is_int(version_id_val)) {
			version_id = yyjson_get_int(version_id_val);
		} else {
			return StringUtil::Format("ViewVersion property 'version_id' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(version_id_val));
		}
	}
	auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp-ms");
	if (!timestamp_ms_val) {
		return "ViewVersion required property 'timestamp-ms' is missing";
	} else {
		if (yyjson_is_sint(timestamp_ms_val)) {
			timestamp_ms = yyjson_get_sint(timestamp_ms_val);
		} else if (yyjson_is_uint(timestamp_ms_val)) {
			timestamp_ms = yyjson_get_uint(timestamp_ms_val);
		} else {
			return StringUtil::Format(
			    "ViewVersion property 'timestamp_ms' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(timestamp_ms_val));
		}
	}
	auto schema_id_val = yyjson_obj_get(obj, "schema-id");
	if (!schema_id_val) {
		return "ViewVersion required property 'schema-id' is missing";
	} else {
		if (yyjson_is_int(schema_id_val)) {
			schema_id = yyjson_get_int(schema_id_val);
		} else {
			return StringUtil::Format("ViewVersion property 'schema_id' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(schema_id_val));
		}
	}
	auto summary_val = yyjson_obj_get(obj, "summary");
	if (!summary_val) {
		return "ViewVersion required property 'summary' is missing";
	} else {
		if (yyjson_is_obj(summary_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(summary_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format("ViewVersion property 'tmp' is not of type 'string', found '%s' instead",
					                          yyjson_get_type_desc(val));
				}
				summary.emplace(key_str, std::move(tmp));
			}
		} else {
			return "ViewVersion property 'summary' is not of type 'object'";
		}
	}
	auto representations_val = yyjson_obj_get(obj, "representations");
	if (!representations_val) {
		return "ViewVersion required property 'representations' is missing";
	} else {
		if (yyjson_is_arr(representations_val)) {
			size_t representations_idx, representations_max;
			yyjson_val *representations_item_val;
			yyjson_arr_foreach(representations_val, representations_idx, representations_max,
			                   representations_item_val) {
				ViewRepresentation representations_item;
				error = representations_item.TryFromJSON(representations_item_val);
				if (!error.empty()) {
					return error;
				}
				representations.emplace_back(std::move(representations_item));
			}
		} else {
			return StringUtil::Format(
			    "ViewVersion property 'representations' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(representations_val));
		}
	}
	auto default_namespace_val = yyjson_obj_get(obj, "default-namespace");
	if (!default_namespace_val) {
		return "ViewVersion required property 'default-namespace' is missing";
	} else {
		error = default_namespace.TryFromJSON(default_namespace_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto default_catalog_val = yyjson_obj_get(obj, "default-catalog");
	if (default_catalog_val) {
		string default_catalog_tmp;
		if (yyjson_is_str(default_catalog_val)) {
			default_catalog_tmp = yyjson_get_str(default_catalog_val);
		} else {
			return StringUtil::Format(
			    "ViewVersion property 'default_catalog_tmp' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(default_catalog_val));
		}
		default_catalog = std::move(default_catalog_tmp);
	}
	return "";
}

void ViewVersion::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: version-id
	yyjson_mut_obj_add_int(doc, obj, "version-id", version_id);

	// Serialize: timestamp-ms
	yyjson_mut_obj_add_sint(doc, obj, "timestamp-ms", timestamp_ms);

	// Serialize: schema-id
	yyjson_mut_obj_add_int(doc, obj, "schema-id", schema_id);

	// Serialize: summary
	yyjson_mut_val *summary_obj = yyjson_mut_obj(doc);
	for (const auto &it : summary) {
		auto &key = it.first;
		auto &value = it.second;
		auto key_ptr = unsafe_yyjson_mut_strncpy(doc, key.c_str(), strlen(key.c_str()));
		yyjson_mut_obj_add_strcpy(doc, summary_obj, key_ptr, value.c_str());
	}
	yyjson_mut_obj_add_val(doc, obj, "summary", summary_obj);

	// Serialize: representations
	yyjson_mut_val *representations_arr = yyjson_mut_arr(doc);
	for (const auto &item : representations) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(representations_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "representations", representations_arr);

	// Serialize: default-namespace
	yyjson_mut_val *default_namespace_val = default_namespace.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "default-namespace", default_namespace_val);

	// Serialize: default-catalog
	if (default_catalog.has_value()) {
		auto &default_catalog_value = *default_catalog;
		yyjson_mut_obj_add_strcpy(doc, obj, "default-catalog", default_catalog_value.c_str());
	}
}

yyjson_mut_val *ViewVersion::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
