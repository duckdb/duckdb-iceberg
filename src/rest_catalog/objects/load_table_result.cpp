
#include "rest_catalog/objects/load_table_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

LoadTableResult::LoadTableResult() {
}

LoadTableResult LoadTableResult::FromJSON(yyjson_val *obj) {
	LoadTableResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string LoadTableResult::TryFromJSON(yyjson_val *obj) {
	string error;
	auto metadata_val = yyjson_obj_get(obj, "metadata");
	if (!metadata_val) {
		return "LoadTableResult required property 'metadata' is missing";
	} else {
		error = metadata.TryFromJSON(metadata_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto metadata_location_val = yyjson_obj_get(obj, "metadata-location");
	if (metadata_location_val) {
		has_metadata_location = true;
		metadata_location = yyjson_get_str(metadata_location_val);
	}
	auto config_val = yyjson_obj_get(obj, "config");
	if (config_val) {
		has_config = true;
		config = parse_object_of_strings(config_val);
	}
	auto storage_credentials_val = yyjson_obj_get(obj, "storage-credentials");
	if (storage_credentials_val) {
		has_storage_credentials = true;
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(storage_credentials_val, idx, max, val) {
			StorageCredential tmp;
			error = tmp.TryFromJSON(val);
			if (!error.empty()) {
				return error;
			}
			storage_credentials.emplace_back(std::move(tmp));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
