
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/oauth_client_credentials_request.hpp"
#include "rest_catalog/objects/oauth_token_exchange_request.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class OAuthTokenRequest {
public:
	OAuthTokenRequest();
	OAuthTokenRequest(const OAuthTokenRequest &) = delete;
	OAuthTokenRequest &operator=(const OAuthTokenRequest &) = delete;
	OAuthTokenRequest(OAuthTokenRequest &&) = default;
	OAuthTokenRequest &operator=(OAuthTokenRequest &&) = default;

public:
	static OAuthTokenRequest FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	OAuthClientCredentialsRequest oauth_client_credentials_request;
	bool has_oauth_client_credentials_request = false;
	OAuthTokenExchangeRequest oauth_token_exchange_request;
	bool has_oauth_token_exchange_request = false;
};

} // namespace rest_api_objects
} // namespace duckdb
