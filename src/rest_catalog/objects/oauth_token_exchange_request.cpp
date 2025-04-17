
#include "rest_catalog/objects/oauth_token_exchange_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

OAuthTokenExchangeRequest::OAuthTokenExchangeRequest() {
}

OAuthTokenExchangeRequest OAuthTokenExchangeRequest::FromJSON(yyjson_val *obj) {
	OAuthTokenExchangeRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string OAuthTokenExchangeRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto grant_type_val = yyjson_obj_get(obj, "grant_type");
	if (!grant_type_val) {
		return "OAuthTokenExchangeRequest required property 'grant_type' is missing";
	} else {
		if (yyjson_is_str(grant_type_val)) {
			grant_type = yyjson_get_str(grant_type_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenExchangeRequest property 'grant_type' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(grant_type_val));
		}
	}
	auto subject_token_val = yyjson_obj_get(obj, "subject_token");
	if (!subject_token_val) {
		return "OAuthTokenExchangeRequest required property 'subject_token' is missing";
	} else {
		if (yyjson_is_str(subject_token_val)) {
			subject_token = yyjson_get_str(subject_token_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenExchangeRequest property 'subject_token' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(subject_token_val));
		}
	}
	auto subject_token_type_val = yyjson_obj_get(obj, "subject_token_type");
	if (!subject_token_type_val) {
		return "OAuthTokenExchangeRequest required property 'subject_token_type' is missing";
	} else {
		error = subject_token_type.TryFromJSON(subject_token_type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto scope_val = yyjson_obj_get(obj, "scope");
	if (scope_val) {
		has_scope = true;
		if (yyjson_is_str(scope_val)) {
			scope = yyjson_get_str(scope_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenExchangeRequest property 'scope' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(scope_val));
		}
	}
	auto requested_token_type_val = yyjson_obj_get(obj, "requested_token_type");
	if (requested_token_type_val) {
		has_requested_token_type = true;
		error = requested_token_type.TryFromJSON(requested_token_type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto actor_token_val = yyjson_obj_get(obj, "actor_token");
	if (actor_token_val) {
		has_actor_token = true;
		if (yyjson_is_str(actor_token_val)) {
			actor_token = yyjson_get_str(actor_token_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenExchangeRequest property 'actor_token' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(actor_token_val));
		}
	}
	auto actor_token_type_val = yyjson_obj_get(obj, "actor_token_type");
	if (actor_token_type_val) {
		has_actor_token_type = true;
		error = actor_token_type.TryFromJSON(actor_token_type_val);
		if (!error.empty()) {
			return error;
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
