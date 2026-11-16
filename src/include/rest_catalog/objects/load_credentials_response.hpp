
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/storage_credential.hpp"

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
	// Deserialization
	static LoadCredentialsResponse FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	LoadCredentialsResponse Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	vector<StorageCredential> storage_credentials;
};

} // namespace rest_api_objects
} // namespace duckdb
