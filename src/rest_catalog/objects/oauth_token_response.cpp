
#include "rest_catalog/objects/oauth_token_response.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

OAuthTokenResponse::OAuthTokenResponse() {
}

OAuthTokenResponse OAuthTokenResponse::FromJSON(yyjson_val *obj) {
	OAuthTokenResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string OAuthTokenResponse::TryFromJSON(yyjson_val *obj) {
	string error;
	auto access_token_val = yyjson_obj_get(obj, "access_token");
	if (!access_token_val) {
		return "OAuthTokenResponse required property 'access_token' is missing";
	} else {
		if (yyjson_is_str(access_token_val)) {
			access_token = yyjson_get_str(access_token_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenResponse property 'access_token' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(access_token_val));
		}
	}
	auto token_type_val = yyjson_obj_get(obj, "token_type");
	if (!token_type_val) {
		return "OAuthTokenResponse required property 'token_type' is missing";
	} else {
		if (yyjson_is_str(token_type_val)) {
			token_type = yyjson_get_str(token_type_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenResponse property 'token_type' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(token_type_val));
		}
	}
	auto expires_in_val = yyjson_obj_get(obj, "expires_in");
	if (expires_in_val) {
		has_expires_in = true;
		if (yyjson_is_int(expires_in_val)) {
			expires_in = yyjson_get_int(expires_in_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenResponse property 'expires_in' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(expires_in_val));
		}
	}
	auto issued_token_type_val = yyjson_obj_get(obj, "issued_token_type");
	if (issued_token_type_val) {
		has_issued_token_type = true;
		error = issued_token_type.TryFromJSON(issued_token_type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto refresh_token_val = yyjson_obj_get(obj, "refresh_token");
	if (refresh_token_val) {
		has_refresh_token = true;
		if (yyjson_is_str(refresh_token_val)) {
			refresh_token = yyjson_get_str(refresh_token_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenResponse property 'refresh_token' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(refresh_token_val));
		}
	}
	auto scope_val = yyjson_obj_get(obj, "scope");
	if (scope_val) {
		has_scope = true;
		if (yyjson_is_str(scope_val)) {
			scope = yyjson_get_str(scope_val);
		} else {
			return StringUtil::Format("OAuthTokenResponse property 'scope' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(scope_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
