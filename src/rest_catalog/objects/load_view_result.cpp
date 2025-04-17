
#include "rest_catalog/objects/load_view_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

LoadViewResult::LoadViewResult() {
}

LoadViewResult LoadViewResult::FromJSON(yyjson_val *obj) {
	LoadViewResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string LoadViewResult::TryFromJSON(yyjson_val *obj) {
	string error;
	auto metadata_location_val = yyjson_obj_get(obj, "metadata-location");
	if (!metadata_location_val) {
		return "LoadViewResult required property 'metadata-location' is missing";
	} else {
		if (yyjson_is_str(metadata_location_val)) {
			metadata_location = yyjson_get_str(metadata_location_val);
		} else {
			return StringUtil::Format(
			    "LoadViewResult property 'metadata_location' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(metadata_location_val));
		}
	}
	auto metadata_val = yyjson_obj_get(obj, "metadata");
	if (!metadata_val) {
		return "LoadViewResult required property 'metadata' is missing";
	} else {
		error = metadata.TryFromJSON(metadata_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto config_val = yyjson_obj_get(obj, "config");
	if (config_val) {
		has_config = true;
		if (yyjson_is_obj(config_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(config_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "LoadViewResult property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				config.emplace(key_str, std::move(tmp));
			}
		} else {
			return "LoadViewResult property 'config' is not of type 'object'";
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
