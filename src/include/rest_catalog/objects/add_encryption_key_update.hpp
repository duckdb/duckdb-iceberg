
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/encrypted_key.hpp"

using namespace duckdb_yyjson;

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
	static AddEncryptionKeyUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	BaseUpdate base_update;
	EncryptedKey encryption_key;
	string action;
	bool has_action = false;
};

} // namespace rest_api_objects
} // namespace duckdb
