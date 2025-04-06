
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/storage_credential.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class LoadCredentialsResponse {
public:
	LoadCredentialsResponse() {
	}

public:
	static LoadCredentialsResponse FromJSON(yyjson_val *obj) {
		LoadCredentialsResponse res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		auto storage_credentials_val = yyjson_obj_get(obj, "storage_credentials");
		if (!storage_credentials_val) {
			return "LoadCredentialsResponse required property 'storage_credentials' is missing";
		} else {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(storage_credentials_val, idx, max, val) {
				StorageCredential tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				storage_credentials.push_back(tmp);
			}
		}
		return string();
	}

public:
	vector<StorageCredential> storage_credentials;
};

} // namespace rest_api_objects
} // namespace duckdb
