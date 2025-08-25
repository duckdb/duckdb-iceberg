
#include "rest_catalog/objects/add_encryption_key_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AddEncryptionKeyUpdate::AddEncryptionKeyUpdate() {
}

AddEncryptionKeyUpdate AddEncryptionKeyUpdate::FromJSON(yyjson_val *obj) {
	AddEncryptionKeyUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string AddEncryptionKeyUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto encryption_key_val = yyjson_obj_get(obj, "encryption-key");
	if (!encryption_key_val) {
		return "AddEncryptionKeyUpdate required property 'encryption-key' is missing";
	} else {
		error = encryption_key.TryFromJSON(encryption_key_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		has_action = true;
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return StringUtil::Format(
			    "AddEncryptionKeyUpdate property 'action' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
