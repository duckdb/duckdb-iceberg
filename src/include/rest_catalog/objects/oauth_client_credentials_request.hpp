
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

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
	static OAuthClientCredentialsRequest FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string grant_type;
	string client_id;
	string client_secret;
	string scope;
	bool has_scope = false;
};

} // namespace rest_api_objects
} // namespace duckdb
