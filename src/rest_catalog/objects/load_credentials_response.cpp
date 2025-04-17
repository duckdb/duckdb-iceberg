
#include "rest_catalog/objects/load_credentials_response.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

LoadCredentialsResponse::LoadCredentialsResponse() {
}

LoadCredentialsResponse LoadCredentialsResponse::FromJSON(yyjson_val *obj) {
	LoadCredentialsResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string LoadCredentialsResponse::TryFromJSON(yyjson_val *obj) {
	string error;
	auto storage_credentials_val = yyjson_obj_get(obj, "storage-credentials");
	if (!storage_credentials_val) {
		return "LoadCredentialsResponse required property 'storage-credentials' is missing";
	} else {
		if (yyjson_is_arr(storage_credentials_val)) {
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
		} else {
			return StringUtil::Format(
			    "LoadCredentialsResponse property 'storage_credentials' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(storage_credentials_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
