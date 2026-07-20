
#include "rest_catalog/objects/remove_encryption_key_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

RemoveEncryptionKeyUpdate::RemoveEncryptionKeyUpdate() {
}

RemoveEncryptionKeyUpdate RemoveEncryptionKeyUpdate::FromJSON(yyjson_val *obj) {
	RemoveEncryptionKeyUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

RemoveEncryptionKeyUpdate RemoveEncryptionKeyUpdate::Copy() const {
	RemoveEncryptionKeyUpdate res;
	res.base_update = base_update.Copy();
	res.key_id = key_id;
	return res;
}

string RemoveEncryptionKeyUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto action_refinement_val = yyjson_obj_get(obj, "action");
	if (action_refinement_val) {
		string action_refinement;
		if (yyjson_is_str(action_refinement_val)) {
			action_refinement = yyjson_get_str(action_refinement_val);
		} else {
			return StringUtil::Format(
			    "RemoveEncryptionKeyUpdate property 'action_refinement' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_refinement_val));
		}
		if (!yyjson_is_null(action_refinement_val) && action_refinement != "remove-encryption-key") {
			return "RemoveEncryptionKeyUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "RemoveEncryptionKeyUpdate required property 'action' is missing";
	}
	auto key_id_val = yyjson_obj_get(obj, "key-id");
	if (!key_id_val) {
		return "RemoveEncryptionKeyUpdate required property 'key-id' is missing";
	} else {
		if (yyjson_is_str(key_id_val)) {
			key_id = yyjson_get_str(key_id_val);
		} else {
			return StringUtil::Format(
			    "RemoveEncryptionKeyUpdate property 'key_id' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(key_id_val));
		}
	}
	return "";
}

void RemoveEncryptionKeyUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: key-id
	yyjson_mut_obj_add_strcpy(doc, obj, "key-id", key_id.c_str());
}

yyjson_mut_val *RemoveEncryptionKeyUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
