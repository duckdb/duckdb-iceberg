
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class OAuthClientCredentialsRequest {
public:
	OAuthClientCredentialsRequest();
	OAuthClientCredentialsRequest(const OAuthClientCredentialsRequest &) = delete;
	OAuthClientCredentialsRequest &operator=(const OAuthClientCredentialsRequest &) = delete;
	OAuthClientCredentialsRequest(OAuthClientCredentialsRequest &&) = default;
	OAuthClientCredentialsRequest &operator=(OAuthClientCredentialsRequest &&) = default;

public:
	// Deserialization
	static OAuthClientCredentialsRequest FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	OAuthClientCredentialsRequest Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string grant_type;
	string client_id;
	string client_secret;
	optional<string> scope;
};

} // namespace rest_api_objects
} // namespace duckdb
