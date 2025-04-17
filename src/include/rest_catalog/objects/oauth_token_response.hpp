
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
	OAuthTokenResponse();
	OAuthTokenResponse(const OAuthTokenResponse &) = delete;
	OAuthTokenResponse &operator=(const OAuthTokenResponse &) = delete;
	OAuthTokenResponse(OAuthTokenResponse &&) = default;
	OAuthTokenResponse &operator=(OAuthTokenResponse &&) = default;

public:
	static OAuthTokenResponse FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string access_token;
	string token_type;
	int64_t expires_in;
	bool has_expires_in = false;
	TokenType issued_token_type;
	bool has_issued_token_type = false;
	string refresh_token;
	bool has_refresh_token = false;
	string scope;
	bool has_scope = false;
};

} // namespace rest_api_objects
} // namespace duckdb
