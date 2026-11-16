
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class StorageCredential {
public:
	StorageCredential();
	StorageCredential(const StorageCredential &) = delete;
	StorageCredential &operator=(const StorageCredential &) = delete;
	StorageCredential(StorageCredential &&) = default;
	StorageCredential &operator=(StorageCredential &&) = default;

public:
	// Deserialization
	static StorageCredential FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	StorageCredential Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string prefix;
	unordered_map<string, string> config;
};

} // namespace rest_api_objects
} // namespace duckdb
