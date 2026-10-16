
#include "rest_catalog/objects/load_view_result.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

LoadViewResult::LoadViewResult() {
}

LoadViewResult LoadViewResult::FromJSON(JSONValue obj) {
	LoadViewResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

LoadViewResult LoadViewResult::Copy() const {
	LoadViewResult res;
	res.metadata_location = metadata_location;
	res.metadata = metadata.Copy();
	if (config.has_value()) {
		res.config.emplace();
		for (auto &entry : (*config)) {
			(*res.config).emplace(entry.first, entry.second);
		}
	}
	if (labels.has_value()) {
		res.labels.emplace();
		(*res.labels) = (*labels).Copy();
	}
	return res;
}

string LoadViewResult::TryFromJSON(JSONValue obj) {
	string error;
	auto metadata_location_val = obj.GetMember("metadata-location");
	if (!metadata_location_val.IsValid()) {
		return "LoadViewResult required property 'metadata-location' is missing";
	} else {
		if (json_utils::IsString(metadata_location_val)) {
			metadata_location = json_utils::GetString(metadata_location_val);
		} else {
			return StringUtil::Format(
			    "LoadViewResult property 'metadata_location' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(metadata_location_val).c_str());
		}
	}
	auto metadata_val = obj.GetMember("metadata");
	if (!metadata_val.IsValid()) {
		return "LoadViewResult required property 'metadata' is missing";
	} else {
		error = metadata.TryFromJSON(metadata_val);
		if (!error.empty()) {
			return error;
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
					    StringUtil::Format("LoadViewResult property 'tmp' is not of type 'string', found %s instead",
					                       json_utils::GetTypeDescription(val).c_str());
					return;
				}
				config_tmp.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "LoadViewResult property 'config_tmp' is not of type 'object'";
		}
		config = std::move(config_tmp);
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

void LoadViewResult::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: metadata-location
	auto metadata_location_json = writer.CreateString(metadata_location);
	obj.Add("metadata-location", metadata_location_json);

	// Serialize: metadata
	auto metadata_json = metadata.ToJSON(writer);
	obj.Add("metadata", metadata_json);

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

	// Serialize: labels
	if (labels.has_value()) {
		auto &labels_value = *labels;
		auto labels_json = labels_value.ToJSON(writer);
		obj.Add("labels", labels_json);
	}
}

JSONMutableValue LoadViewResult::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
