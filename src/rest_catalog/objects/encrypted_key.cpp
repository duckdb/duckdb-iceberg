
#include "rest_catalog/objects/encrypted_key.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

EncryptedKey::EncryptedKey() {
}

EncryptedKey EncryptedKey::FromJSON(yyjson_val *obj) {
	EncryptedKey res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string EncryptedKey::TryFromJSON(yyjson_val *obj) {
	string error;
	auto key_id_val = yyjson_obj_get(obj, "key-id");
	if (!key_id_val) {
		return "EncryptedKey required property 'key-id' is missing";
	} else {
		if (yyjson_is_str(key_id_val)) {
			key_id = yyjson_get_str(key_id_val);
		} else {
			return StringUtil::Format("EncryptedKey property 'key_id' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(key_id_val));
		}
	}
	auto encrypted_key_metadata_val = yyjson_obj_get(obj, "encrypted-key-metadata");
	if (!encrypted_key_metadata_val) {
		return "EncryptedKey required property 'encrypted-key-metadata' is missing";
	} else {
		if (yyjson_is_str(encrypted_key_metadata_val)) {
			encrypted_key_metadata = yyjson_get_str(encrypted_key_metadata_val);
		} else {
			return StringUtil::Format(
			    "EncryptedKey property 'encrypted_key_metadata' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(encrypted_key_metadata_val));
		}
	}
	auto encrypted_by_id_val = yyjson_obj_get(obj, "encrypted-by-id");
	if (encrypted_by_id_val) {
		has_encrypted_by_id = true;
		if (yyjson_is_str(encrypted_by_id_val)) {
			encrypted_by_id = yyjson_get_str(encrypted_by_id_val);
		} else {
			return StringUtil::Format(
			    "EncryptedKey property 'encrypted_by_id' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(encrypted_by_id_val));
		}
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		has_properties = true;
		if (yyjson_is_obj(properties_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(properties_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format("EncryptedKey property 'tmp' is not of type 'string', found '%s' instead",
					                          yyjson_get_type_desc(val));
				}
				properties.emplace(key_str, std::move(tmp));
			}
		} else {
			return "EncryptedKey property 'properties' is not of type 'object'";
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
