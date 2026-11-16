
#include "rest_catalog/objects/storage_credential.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

StorageCredential::StorageCredential() {
}

StorageCredential StorageCredential::FromJSON(JSONValue obj) {
	StorageCredential res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

StorageCredential StorageCredential::Copy() const {
	StorageCredential res;
	res.prefix = prefix;
	for (auto &entry : config) {
		res.config.emplace(entry.first, entry.second);
	}
	return res;
}

string StorageCredential::TryFromJSON(JSONValue obj) {
	string error;
	auto prefix_val = obj.GetMember("prefix");
	if (!prefix_val.IsValid()) {
		return "StorageCredential required property 'prefix' is missing";
	} else {
		if (json_utils::IsString(prefix_val)) {
			prefix = json_utils::GetString(prefix_val);
		} else {
			return StringUtil::Format("StorageCredential property 'prefix' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(prefix_val).c_str());
		}
	}
	auto config_val = obj.GetMember("config");
	if (!config_val.IsValid()) {
		return "StorageCredential required property 'config' is missing";
	} else {
		if (config_val.IsObject()) {
			config_val.IterateObject([&](const string &key_str, JSONValue val) {
				if (!error.empty()) {
					return;
				}
				string tmp;
				if (json_utils::IsString(val)) {
					tmp = json_utils::GetString(val);
				} else {
					error =
					    StringUtil::Format("StorageCredential property 'tmp' is not of type 'string', found %s instead",
					                       json_utils::GetTypeDescription(val).c_str());
					return;
				}
				config.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "StorageCredential property 'config' is not of type 'object'";
		}
	}
	return "";
}

void StorageCredential::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: prefix
	auto prefix_json = writer.CreateString(prefix);
	obj.Add("prefix", prefix_json);

	// Serialize: config
	auto config_json = writer.CreateObject();
	for (const auto &[config_json_key, config_json_value] : config) {
		auto config_json_value_json = writer.CreateString(config_json_value);
		config_json.Add(config_json_key, config_json_value_json);
	}
	obj.Add("config", config_json);
}

JSONMutableValue StorageCredential::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
