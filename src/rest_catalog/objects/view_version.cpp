
#include "rest_catalog/objects/view_version.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
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

string ViewVersion::TryFromJSON(yyjson_val *obj) {
	string error;
	auto version_id_val = yyjson_obj_get(obj, "version-id");
	if (!version_id_val) {
		return "ViewVersion required property 'version-id' is missing";
	} else {
		version_id = yyjson_get_sint(version_id_val);
	}
	auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp-ms");
	if (!timestamp_ms_val) {
		return "ViewVersion required property 'timestamp-ms' is missing";
	} else {
		timestamp_ms = yyjson_get_sint(timestamp_ms_val);
	}
	auto schema_id_val = yyjson_obj_get(obj, "schema-id");
	if (!schema_id_val) {
		return "ViewVersion required property 'schema-id' is missing";
	} else {
		schema_id = yyjson_get_sint(schema_id_val);
	}
	auto summary_val = yyjson_obj_get(obj, "summary");
	if (!summary_val) {
		return "ViewVersion required property 'summary' is missing";
	} else {
		summary = parse_object_of_strings(summary_val);
	}
	auto representations_val = yyjson_obj_get(obj, "representations");
	if (!representations_val) {
		return "ViewVersion required property 'representations' is missing";
	} else {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(representations_val, idx, max, val) {
			ViewRepresentation tmp;
			error = tmp.TryFromJSON(val);
			if (!error.empty()) {
				return error;
			}
			representations.emplace_back(std::move(tmp));
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
		has_default_catalog = true;
		default_catalog = yyjson_get_str(default_catalog_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
