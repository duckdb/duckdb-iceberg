
#include "rest_catalog/objects/oauth_client_credentials_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

OAuthClientCredentialsRequest::OAuthClientCredentialsRequest() {
}

OAuthClientCredentialsRequest OAuthClientCredentialsRequest::FromJSON(yyjson_val *obj) {
	OAuthClientCredentialsRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string OAuthClientCredentialsRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto grant_type_val = yyjson_obj_get(obj, "grant_type");
	if (!grant_type_val) {
		return "OAuthClientCredentialsRequest required property 'grant_type' is missing";
	} else {
		if (yyjson_is_str(grant_type_val)) {
			grant_type = yyjson_get_str(grant_type_val);
		} else {
			return StringUtil::Format(
			    "OAuthClientCredentialsRequest property 'grant_type' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(grant_type_val));
		}
	}
	auto client_id_val = yyjson_obj_get(obj, "client_id");
	if (!client_id_val) {
		return "OAuthClientCredentialsRequest required property 'client_id' is missing";
	} else {
		if (yyjson_is_str(client_id_val)) {
			client_id = yyjson_get_str(client_id_val);
		} else {
			return StringUtil::Format(
			    "OAuthClientCredentialsRequest property 'client_id' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(client_id_val));
		}
	}
	auto client_secret_val = yyjson_obj_get(obj, "client_secret");
	if (!client_secret_val) {
		return "OAuthClientCredentialsRequest required property 'client_secret' is missing";
	} else {
		if (yyjson_is_str(client_secret_val)) {
			client_secret = yyjson_get_str(client_secret_val);
		} else {
			return StringUtil::Format(
			    "OAuthClientCredentialsRequest property 'client_secret' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(client_secret_val));
		}
	}
	auto scope_val = yyjson_obj_get(obj, "scope");
	if (scope_val) {
		has_scope = true;
		if (yyjson_is_str(scope_val)) {
			scope = yyjson_get_str(scope_val);
		} else {
			return StringUtil::Format(
			    "OAuthClientCredentialsRequest property 'scope' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(scope_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
