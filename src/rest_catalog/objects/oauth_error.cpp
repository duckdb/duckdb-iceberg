
#include "rest_catalog/objects/oauth_error.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

OAuthError::OAuthError() {
}

OAuthError OAuthError::FromJSON(yyjson_val *obj) {
	OAuthError res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string OAuthError::TryFromJSON(yyjson_val *obj) {
	string error;
	auto _error_val = yyjson_obj_get(obj, "error");
	if (!_error_val) {
		return "OAuthError required property 'error' is missing";
	} else {
		if (yyjson_is_str(_error_val)) {
			_error = yyjson_get_str(_error_val);
		} else {
			return StringUtil::Format("OAuthError property '_error' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(_error_val));
		}
	}
	auto error_description_val = yyjson_obj_get(obj, "error_description");
	if (error_description_val) {
		has_error_description = true;
		if (yyjson_is_str(error_description_val)) {
			error_description = yyjson_get_str(error_description_val);
		} else {
			return StringUtil::Format(
			    "OAuthError property 'error_description' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(error_description_val));
		}
	}
	auto error_uri_val = yyjson_obj_get(obj, "error_uri");
	if (error_uri_val) {
		has_error_uri = true;
		if (yyjson_is_str(error_uri_val)) {
			error_uri = yyjson_get_str(error_uri_val);
		} else {
			return StringUtil::Format("OAuthError property 'error_uri' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(error_uri_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
