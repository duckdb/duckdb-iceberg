
#include "rest_catalog/objects/load_credentials_response.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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

LoadCredentialsResponse LoadCredentialsResponse::Copy() const {
	LoadCredentialsResponse res;
	res.storage_credentials.reserve(storage_credentials.size());
	for (auto &item : storage_credentials) {
		res.storage_credentials.emplace_back(item.Copy());
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
			size_t storage_credentials_idx, storage_credentials_max;
			yyjson_val *storage_credentials_item_val;
			yyjson_arr_foreach(storage_credentials_val, storage_credentials_idx, storage_credentials_max,
			                   storage_credentials_item_val) {
				StorageCredential storage_credentials_item;
				error = storage_credentials_item.TryFromJSON(storage_credentials_item_val);
				if (!error.empty()) {
					return error;
				}
				storage_credentials.emplace_back(std::move(storage_credentials_item));
			}
		} else {
			return StringUtil::Format(
			    "LoadCredentialsResponse property 'storage_credentials' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(storage_credentials_val));
		}
	}
	return "";
}

void LoadCredentialsResponse::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: storage-credentials
	yyjson_mut_val *storage_credentials_arr = yyjson_mut_arr(doc);
	for (const auto &item : storage_credentials) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(storage_credentials_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "storage-credentials", storage_credentials_arr);
}

yyjson_mut_val *LoadCredentialsResponse::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
