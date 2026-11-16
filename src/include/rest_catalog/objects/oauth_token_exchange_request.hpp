
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/token_type.hpp"

namespace duckdb {
namespace rest_api_objects {

class OAuthTokenExchangeRequest {
public:
	OAuthTokenExchangeRequest();
	OAuthTokenExchangeRequest(const OAuthTokenExchangeRequest &) = delete;
	OAuthTokenExchangeRequest &operator=(const OAuthTokenExchangeRequest &) = delete;
	OAuthTokenExchangeRequest(OAuthTokenExchangeRequest &&) = default;
	OAuthTokenExchangeRequest &operator=(OAuthTokenExchangeRequest &&) = default;

public:
	// Deserialization
	static OAuthTokenExchangeRequest FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	OAuthTokenExchangeRequest Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string grant_type;
	string subject_token;
	TokenType subject_token_type;
	optional<string> scope;
	optional<TokenType> requested_token_type;
	optional<string> actor_token;
	optional<TokenType> actor_token_type;
};

} // namespace rest_api_objects
} // namespace duckdb
