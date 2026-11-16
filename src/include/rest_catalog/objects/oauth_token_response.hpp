
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/token_type.hpp"

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
	// Deserialization
	static OAuthTokenResponse FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	OAuthTokenResponse Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string access_token;
	string token_type;
	optional<int32_t> expires_in;
	optional<TokenType> issued_token_type;
	optional<string> refresh_token;
	optional<string> scope;
};

} // namespace rest_api_objects
} // namespace duckdb
