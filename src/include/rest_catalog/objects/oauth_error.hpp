
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class OAuthError {
public:
	OAuthError() {
	}

public:
	static OAuthError FromJSON(yyjson_val *obj) {
		OAuthError res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		auto _error_val = yyjson_obj_get(obj, "_error");
		if (!_error_val) {
			return "OAuthError required property '_error' is missing";
		} else {
			_error = yyjson_get_str(_error_val);
		}
		auto error_description_val = yyjson_obj_get(obj, "error_description");
		if (error_description_val) {
			error_description = yyjson_get_str(error_description_val);
		}
		auto error_uri_val = yyjson_obj_get(obj, "error_uri");
		if (error_uri_val) {
			error_uri = yyjson_get_str(error_uri_val);
		}
		return string();
	}

public:
	string _error;
	string error_description;
	string error_uri;
};

} // namespace rest_api_objects
} // namespace duckdb
