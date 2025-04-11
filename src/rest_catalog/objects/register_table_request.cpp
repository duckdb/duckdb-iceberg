
#include "rest_catalog/objects/register_table_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

RegisterTableRequest::RegisterTableRequest() {
}

RegisterTableRequest RegisterTableRequest::FromJSON(yyjson_val *obj) {
	RegisterTableRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string RegisterTableRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto name_val = yyjson_obj_get(obj, "name");
	if (!name_val) {
		return "RegisterTableRequest required property 'name' is missing";
	} else {
		name = yyjson_get_str(name_val);
	}
	auto metadata_location_val = yyjson_obj_get(obj, "metadata-location");
	if (!metadata_location_val) {
		return "RegisterTableRequest required property 'metadata-location' is missing";
	} else {
		metadata_location = yyjson_get_str(metadata_location_val);
	}
	auto overwrite_val = yyjson_obj_get(obj, "overwrite");
	if (overwrite_val) {
		has_overwrite = true;
		overwrite = yyjson_get_bool(overwrite_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
