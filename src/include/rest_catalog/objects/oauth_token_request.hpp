
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/oauth_client_credentials_request.hpp"
#include "rest_catalog/objects/oauth_token_exchange_request.hpp"

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
	// Deserialization
	static OAuthTokenRequest FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	OAuthTokenRequest Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<OAuthClientCredentialsRequest> oauth_client_credentials_request;
	optional<OAuthTokenExchangeRequest> oauth_token_exchange_request;
};

} // namespace rest_api_objects
} // namespace duckdb
