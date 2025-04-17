
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/storage_credential.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class LoadCredentialsResponse {
public:
	LoadCredentialsResponse();
	LoadCredentialsResponse(const LoadCredentialsResponse &) = delete;
	LoadCredentialsResponse &operator=(const LoadCredentialsResponse &) = delete;
	LoadCredentialsResponse(LoadCredentialsResponse &&) = default;
	LoadCredentialsResponse &operator=(LoadCredentialsResponse &&) = default;

public:
	static LoadCredentialsResponse FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	vector<StorageCredential> storage_credentials;
};

} // namespace rest_api_objects
} // namespace duckdb
