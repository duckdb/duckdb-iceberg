
#include "rest_catalog/objects/encrypted_key.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

EncryptedKey::EncryptedKey() {
}

EncryptedKey EncryptedKey::FromJSON(JSONValue obj) {
	EncryptedKey res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

EncryptedKey EncryptedKey::Copy() const {
	EncryptedKey res;
	res.key_id = key_id;
	res.encrypted_key_metadata = encrypted_key_metadata;
	if (encrypted_by_id.has_value()) {
		res.encrypted_by_id.emplace();
		(*res.encrypted_by_id) = (*encrypted_by_id);
	}
	if (properties.has_value()) {
		res.properties.emplace();
		for (auto &entry : (*properties)) {
			(*res.properties).emplace(entry.first, entry.second);
		}
	}
	return res;
}

string EncryptedKey::TryFromJSON(JSONValue obj) {
	string error;
	auto key_id_val = obj.GetMember("key-id");
	if (!key_id_val.IsValid()) {
		return "EncryptedKey required property 'key-id' is missing";
	} else {
		if (json_utils::IsString(key_id_val)) {
			key_id = json_utils::GetString(key_id_val);
		} else {
			return StringUtil::Format("EncryptedKey property 'key_id' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(key_id_val).c_str());
		}
	}
	auto encrypted_key_metadata_val = obj.GetMember("encrypted-key-metadata");
	if (!encrypted_key_metadata_val.IsValid()) {
		return "EncryptedKey required property 'encrypted-key-metadata' is missing";
	} else {
		if (json_utils::IsString(encrypted_key_metadata_val)) {
			encrypted_key_metadata = json_utils::GetString(encrypted_key_metadata_val);
		} else {
			return StringUtil::Format(
			    "EncryptedKey property 'encrypted_key_metadata' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(encrypted_key_metadata_val).c_str());
		}
	}
	auto encrypted_by_id_val = obj.GetMember("encrypted-by-id");
	if (encrypted_by_id_val.IsValid()) {
		string encrypted_by_id_tmp;
		if (json_utils::IsString(encrypted_by_id_val)) {
			encrypted_by_id_tmp = json_utils::GetString(encrypted_by_id_val);
		} else {
			return StringUtil::Format(
			    "EncryptedKey property 'encrypted_by_id_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(encrypted_by_id_val).c_str());
		}
		encrypted_by_id = std::move(encrypted_by_id_tmp);
	}
	auto properties_val = obj.GetMember("properties");
	if (properties_val.IsValid()) {
		unordered_map<string, string> properties_tmp;
		if (properties_val.IsObject()) {
			properties_val.IterateObject([&](const string &key_str, JSONValue val) {
				if (!error.empty()) {
					return;
				}
				string tmp;
				if (json_utils::IsString(val)) {
					tmp = json_utils::GetString(val);
				} else {
					error = StringUtil::Format("EncryptedKey property 'tmp' is not of type 'string', found %s instead",
					                           json_utils::GetTypeDescription(val).c_str());
					return;
				}
				properties_tmp.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "EncryptedKey property 'properties_tmp' is not of type 'object'";
		}
		properties = std::move(properties_tmp);
	}
	return "";
}

void EncryptedKey::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: key-id
	auto key_id_json = writer.CreateString(key_id);
	obj.Add("key-id", key_id_json);

	// Serialize: encrypted-key-metadata
	auto encrypted_key_metadata_json = writer.CreateString(encrypted_key_metadata);
	obj.Add("encrypted-key-metadata", encrypted_key_metadata_json);

	// Serialize: encrypted-by-id
	if (encrypted_by_id.has_value()) {
		auto &encrypted_by_id_value = *encrypted_by_id;
		auto encrypted_by_id_json = writer.CreateString(encrypted_by_id_value);
		obj.Add("encrypted-by-id", encrypted_by_id_json);
	}

	// Serialize: properties
	if (properties.has_value()) {
		auto &properties_value = *properties;
		auto properties_json = writer.CreateObject();
		for (const auto &[properties_json_key, properties_json_value] : properties_value) {
			auto properties_json_value_json = writer.CreateString(properties_json_value);
			properties_json.Add(properties_json_key, properties_json_value_json);
		}
		obj.Add("properties", properties_json);
	}
}

JSONMutableValue EncryptedKey::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
