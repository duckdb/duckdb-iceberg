
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class EncryptedKey {
public:
	EncryptedKey();
	EncryptedKey(const EncryptedKey &) = delete;
	EncryptedKey &operator=(const EncryptedKey &) = delete;
	EncryptedKey(EncryptedKey &&) = default;
	EncryptedKey &operator=(EncryptedKey &&) = default;

public:
	// Deserialization
	static EncryptedKey FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	EncryptedKey Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string key_id;
	string encrypted_key_metadata;
	optional<string> encrypted_by_id;
	optional<unordered_map<string, string>> properties;
};

} // namespace rest_api_objects
} // namespace duckdb
