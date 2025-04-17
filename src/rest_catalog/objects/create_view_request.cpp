
#include "rest_catalog/objects/create_view_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

CreateViewRequest::CreateViewRequest() {
}

CreateViewRequest CreateViewRequest::FromJSON(yyjson_val *obj) {
	CreateViewRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string CreateViewRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto name_val = yyjson_obj_get(obj, "name");
	if (!name_val) {
		return "CreateViewRequest required property 'name' is missing";
	} else {
		if (yyjson_is_str(name_val)) {
			name = yyjson_get_str(name_val);
		} else {
			return StringUtil::Format("CreateViewRequest property 'name' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(name_val));
		}
	}
	auto schema_val = yyjson_obj_get(obj, "schema");
	if (!schema_val) {
		return "CreateViewRequest required property 'schema' is missing";
	} else {
		error = schema.TryFromJSON(schema_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto view_version_val = yyjson_obj_get(obj, "view-version");
	if (!view_version_val) {
		return "CreateViewRequest required property 'view-version' is missing";
	} else {
		error = view_version.TryFromJSON(view_version_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (!properties_val) {
		return "CreateViewRequest required property 'properties' is missing";
	} else {
		if (yyjson_is_obj(properties_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(properties_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "CreateViewRequest property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				properties.emplace(key_str, std::move(tmp));
			}
		} else {
			return "CreateViewRequest property 'properties' is not of type 'object'";
		}
	}
	auto location_val = yyjson_obj_get(obj, "location");
	if (location_val) {
		has_location = true;
		if (yyjson_is_str(location_val)) {
			location = yyjson_get_str(location_val);
		} else {
			return StringUtil::Format(
			    "CreateViewRequest property 'location' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(location_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
