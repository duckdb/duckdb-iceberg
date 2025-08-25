
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

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
	static EncryptedKey FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string key_id;
	string encrypted_key_metadata;
	string encrypted_by_id;
	bool has_encrypted_by_id = false;
	case_insensitive_map_t<string> properties;
	bool has_properties = false;
};

} // namespace rest_api_objects
} // namespace duckdb
