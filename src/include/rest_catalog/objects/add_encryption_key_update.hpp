
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/encrypted_key.hpp"

namespace duckdb {
namespace rest_api_objects {

class AddEncryptionKeyUpdate {
public:
	AddEncryptionKeyUpdate();
	AddEncryptionKeyUpdate(const AddEncryptionKeyUpdate &) = delete;
	AddEncryptionKeyUpdate &operator=(const AddEncryptionKeyUpdate &) = delete;
	AddEncryptionKeyUpdate(AddEncryptionKeyUpdate &&) = default;
	AddEncryptionKeyUpdate &operator=(AddEncryptionKeyUpdate &&) = default;

public:
	// Deserialization
	static AddEncryptionKeyUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AddEncryptionKeyUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	EncryptedKey encryption_key;
};

} // namespace rest_api_objects
} // namespace duckdb
