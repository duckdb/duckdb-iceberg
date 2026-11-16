
#include "rest_catalog/objects/add_encryption_key_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

AddEncryptionKeyUpdate::AddEncryptionKeyUpdate() {
}

AddEncryptionKeyUpdate AddEncryptionKeyUpdate::FromJSON(JSONValue obj) {
	AddEncryptionKeyUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AddEncryptionKeyUpdate AddEncryptionKeyUpdate::Copy() const {
	AddEncryptionKeyUpdate res;
	res.base_update = base_update.Copy();
	res.encryption_key = encryption_key.Copy();
	return res;
}

string AddEncryptionKeyUpdate::TryFromJSON(JSONValue obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto action_refinement_val = obj.GetMember("action");
	if (action_refinement_val.IsValid()) {
		string action_refinement;
		if (json_utils::IsString(action_refinement_val)) {
			action_refinement = json_utils::GetString(action_refinement_val);
		} else {
			return StringUtil::Format(
			    "AddEncryptionKeyUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "add-encryption-key") {
			return "AddEncryptionKeyUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "AddEncryptionKeyUpdate required property 'action' is missing";
	}
	auto encryption_key_val = obj.GetMember("encryption-key");
	if (!encryption_key_val.IsValid()) {
		return "AddEncryptionKeyUpdate required property 'encryption-key' is missing";
	} else {
		error = encryption_key.TryFromJSON(encryption_key_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void AddEncryptionKeyUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: encryption-key
	auto encryption_key_json = encryption_key.ToJSON(writer);
	obj.Add("encryption-key", encryption_key_json);
}

JSONMutableValue AddEncryptionKeyUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
