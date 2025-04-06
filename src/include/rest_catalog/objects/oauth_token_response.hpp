
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

class OAuthTokenResponse {
public:
	OAuthTokenResponse::OAuthTokenResponse() {
	}

public:
	static OAuthTokenResponse FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto access_token_val = yyjson_obj_get(obj, "access_token");
		if (!access_token_val) {
		return "OAuthTokenResponse required property 'access_token' is missing");
		}
		result.access_token = yyjson_get_str(access_token_val);

		auto token_type_val = yyjson_obj_get(obj, "token_type");
		if (!token_type_val) {
		return "OAuthTokenResponse required property 'token_type' is missing");
		}
		result.token_type = yyjson_get_str(token_type_val);

		auto expires_in_val = yyjson_obj_get(obj, "expires_in");
		if (expires_in_val) {
			result.expires_in = yyjson_get_sint(expires_in_val);
		}

		auto issued_token_type_val = yyjson_obj_get(obj, "issued_token_type");
		if (issued_token_type_val) {
			result.issued_token_type = TokenType::FromJSON(issued_token_type_val);
		}

		auto refresh_token_val = yyjson_obj_get(obj, "refresh_token");
		if (refresh_token_val) {
			result.refresh_token = yyjson_get_str(refresh_token_val);
		}

		auto scope_val = yyjson_obj_get(obj, "scope");
		if (scope_val) {
			result.scope = yyjson_get_str(scope_val);
		}
		return string();
	}

public:
public:
};

} // namespace rest_api_objects
} // namespace duckdb
