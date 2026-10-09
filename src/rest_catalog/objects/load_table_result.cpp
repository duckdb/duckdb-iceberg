
#include "rest_catalog/objects/load_table_result.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

LoadTableResult::LoadTableResult() {
}

LoadTableResult LoadTableResult::FromJSON(JSONValue obj) {
	LoadTableResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

LoadTableResult LoadTableResult::Copy() const {
	LoadTableResult res;
	res.metadata = metadata.Copy();
	if (metadata_location.has_value()) {
		res.metadata_location.emplace();
		(*res.metadata_location) = (*metadata_location);
	}
	if (config.has_value()) {
		res.config.emplace();
		for (auto &entry : (*config)) {
			(*res.config).emplace(entry.first, entry.second);
		}
	}
	if (storage_credentials.has_value()) {
		res.storage_credentials.emplace();
		(*res.storage_credentials).reserve((*storage_credentials).size());
		for (auto &item : (*storage_credentials)) {
			(*res.storage_credentials).emplace_back(item.Copy());
		}
	}
	if (remote_signing_config.has_value()) {
		res.remote_signing_config.emplace();
		(*res.remote_signing_config) = (*remote_signing_config).Copy();
	}
	if (read_restrictions.has_value()) {
		res.read_restrictions.emplace();
		(*res.read_restrictions) = (*read_restrictions).Copy();
	}
	if (labels.has_value()) {
		res.labels.emplace();
		(*res.labels) = (*labels).Copy();
	}
	return res;
}

string LoadTableResult::TryFromJSON(JSONValue obj) {
	string error;
	auto metadata_val = obj.GetMember("metadata");
	if (!metadata_val.IsValid()) {
		return "LoadTableResult required property 'metadata' is missing";
	} else {
		error = metadata.TryFromJSON(metadata_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto metadata_location_val = obj.GetMember("metadata-location");
	if (metadata_location_val.IsValid()) {
		if (metadata_location_val.IsNull()) {
			//! do nothing, property is explicitly nullable
		} else {
			string metadata_location_tmp;
			if (json_utils::IsString(metadata_location_val)) {
				metadata_location_tmp = json_utils::GetString(metadata_location_val);
			} else {
				return StringUtil::Format(
				    "LoadTableResult property 'metadata_location_tmp' is not of type 'string', found %s instead",
				    json_utils::GetTypeDescription(metadata_location_val).c_str());
			}
			metadata_location = std::move(metadata_location_tmp);
		}
	}
	auto config_val = obj.GetMember("config");
	if (config_val.IsValid()) {
		case_insensitive_map_t<string> config_tmp;
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
					    StringUtil::Format("LoadTableResult property 'tmp' is not of type 'string', found %s instead",
					                       json_utils::GetTypeDescription(val).c_str());
					return;
				}
				config_tmp.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "LoadTableResult property 'config_tmp' is not of type 'object'";
		}
		config = std::move(config_tmp);
	}
	auto storage_credentials_val = obj.GetMember("storage-credentials");
	if (storage_credentials_val.IsValid()) {
		vector<StorageCredential> storage_credentials_tmp;
		if (storage_credentials_val.IsArray()) {
			storage_credentials_val.IterateArray([&](JSONValue storage_credentials_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				StorageCredential storage_credentials_tmp_item;
				error = storage_credentials_tmp_item.TryFromJSON(storage_credentials_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				storage_credentials_tmp.emplace_back(std::move(storage_credentials_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "LoadTableResult property 'storage_credentials_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(storage_credentials_val).c_str());
		}
		storage_credentials = std::move(storage_credentials_tmp);
	}
	auto remote_signing_config_val = obj.GetMember("remote-signing-config");
	if (remote_signing_config_val.IsValid()) {
		RemoteSigningConfig remote_signing_config_tmp;
		error = remote_signing_config_tmp.TryFromJSON(remote_signing_config_val);
		if (!error.empty()) {
			return error;
		}
		remote_signing_config = std::move(remote_signing_config_tmp);
	}
	auto read_restrictions_val = obj.GetMember("read-restrictions");
	if (read_restrictions_val.IsValid()) {
		ReadRestrictions read_restrictions_tmp;
		error = read_restrictions_tmp.TryFromJSON(read_restrictions_val);
		if (!error.empty()) {
			return error;
		}
		read_restrictions = std::move(read_restrictions_tmp);
	}
	auto labels_val = obj.GetMember("labels");
	if (labels_val.IsValid()) {
		Labels labels_tmp;
		error = labels_tmp.TryFromJSON(labels_val);
		if (!error.empty()) {
			return error;
		}
		labels = std::move(labels_tmp);
	}
	return "";
}

void LoadTableResult::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: metadata
	auto metadata_json = metadata.ToJSON(writer);
	obj.Add("metadata", metadata_json);

	// Serialize: metadata-location
	if (metadata_location.has_value()) {
		auto &metadata_location_value = *metadata_location;
		auto metadata_location_json = writer.CreateString(metadata_location_value);
		obj.Add("metadata-location", metadata_location_json);
	}

	// Serialize: config
	if (config.has_value()) {
		auto &config_value = *config;
		auto config_json = writer.CreateObject();
		for (const auto &[config_json_key, config_json_value] : config_value) {
			auto config_json_value_json = writer.CreateString(config_json_value);
			config_json.Add(config_json_key, config_json_value_json);
		}
		obj.Add("config", config_json);
	}

	// Serialize: storage-credentials
	if (storage_credentials.has_value()) {
		auto &storage_credentials_value = *storage_credentials;
		auto storage_credentials_json = writer.CreateArray();
		for (const auto &storage_credentials_json_item : storage_credentials_value) {
			auto storage_credentials_json_item_json = storage_credentials_json_item.ToJSON(writer);
			storage_credentials_json.Append(storage_credentials_json_item_json);
		}
		obj.Add("storage-credentials", storage_credentials_json);
	}

	// Serialize: remote-signing-config
	if (remote_signing_config.has_value()) {
		auto &remote_signing_config_value = *remote_signing_config;
		auto remote_signing_config_json = remote_signing_config_value.ToJSON(writer);
		obj.Add("remote-signing-config", remote_signing_config_json);
	}

	// Serialize: read-restrictions
	if (read_restrictions.has_value()) {
		auto &read_restrictions_value = *read_restrictions;
		auto read_restrictions_json = read_restrictions_value.ToJSON(writer);
		obj.Add("read-restrictions", read_restrictions_json);
	}

	// Serialize: labels
	if (labels.has_value()) {
		auto &labels_value = *labels;
		auto labels_json = labels_value.ToJSON(writer);
		obj.Add("labels", labels_json);
	}
}

JSONMutableValue LoadTableResult::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
