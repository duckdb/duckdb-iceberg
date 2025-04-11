
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class OAuthError {
public:
	OAuthError();
	OAuthError(const OAuthError &) = delete;
	OAuthError &operator=(const OAuthError &) = delete;
	OAuthError(OAuthError &&) = default;
	OAuthError &operator=(OAuthError &&) = default;

public:
	static OAuthError FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string _error;
	string error_description;
	bool has_error_description = false;
	string error_uri;
	bool has_error_uri = false;
};

} // namespace rest_api_objects
} // namespace duckdb
