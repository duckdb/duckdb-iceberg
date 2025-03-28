#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/token_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class OAuthTokenExchangeRequest {
public:
	static OAuthTokenExchangeRequest FromJSON(yyjson_val *obj) {
		OAuthTokenExchangeRequest result;

		auto actor_token_val = yyjson_obj_get(obj, "actor_token");
		if (actor_token_val) {
			result.actor_token = yyjson_get_str(actor_token_val);
		}

		auto actor_token_type_val = yyjson_obj_get(obj, "actor_token_type");
		if (actor_token_type_val) {
			result.actor_token_type = TokenType::FromJSON(actor_token_type_val);
		}

		auto grant_type_val = yyjson_obj_get(obj, "grant_type");
		if (grant_type_val) {
			result.grant_type = yyjson_get_str(grant_type_val);
		}
		else {
			throw IOException("OAuthTokenExchangeRequest required property 'grant_type' is missing");
		}

		auto requested_token_type_val = yyjson_obj_get(obj, "requested_token_type");
		if (requested_token_type_val) {
			result.requested_token_type = TokenType::FromJSON(requested_token_type_val);
		}

		auto scope_val = yyjson_obj_get(obj, "scope");
		if (scope_val) {
			result.scope = yyjson_get_str(scope_val);
		}

		auto subject_token_val = yyjson_obj_get(obj, "subject_token");
		if (subject_token_val) {
			result.subject_token = yyjson_get_str(subject_token_val);
		}
		else {
			throw IOException("OAuthTokenExchangeRequest required property 'subject_token' is missing");
		}

		auto subject_token_type_val = yyjson_obj_get(obj, "subject_token_type");
		if (subject_token_type_val) {
			result.subject_token_type = TokenType::FromJSON(subject_token_type_val);
		}
		else {
			throw IOException("OAuthTokenExchangeRequest required property 'subject_token_type' is missing");
		}

		return result;
	}

public:
	string actor_token;
	TokenType actor_token_type;
	string grant_type;
	TokenType requested_token_type;
	string scope;
	string subject_token;
	TokenType subject_token_type;
};
} // namespace rest_api_objects
} // namespace duckdb