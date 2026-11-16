
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

namespace duckdb {
namespace rest_api_objects {

class RemoveEncryptionKeyUpdate {
public:
	RemoveEncryptionKeyUpdate();
	RemoveEncryptionKeyUpdate(const RemoveEncryptionKeyUpdate &) = delete;
	RemoveEncryptionKeyUpdate &operator=(const RemoveEncryptionKeyUpdate &) = delete;
	RemoveEncryptionKeyUpdate(RemoveEncryptionKeyUpdate &&) = default;
	RemoveEncryptionKeyUpdate &operator=(RemoveEncryptionKeyUpdate &&) = default;

public:
	// Deserialization
	static RemoveEncryptionKeyUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	RemoveEncryptionKeyUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	string key_id;
};

} // namespace rest_api_objects
} // namespace duckdb
