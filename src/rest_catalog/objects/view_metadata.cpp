
#include "rest_catalog/objects/view_metadata.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ViewMetadata::ViewMetadata() {
}

ViewMetadata ViewMetadata::FromJSON(JSONValue obj) {
	ViewMetadata res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ViewMetadata ViewMetadata::Copy() const {
	ViewMetadata res;
	res.view_uuid = view_uuid;
	res.format_version = format_version;
	res.location = location;
	res.current_version_id = current_version_id;
	res.versions.reserve(versions.size());
	for (auto &item : versions) {
		res.versions.emplace_back(item.Copy());
	}
	res.version_log.reserve(version_log.size());
	for (auto &item : version_log) {
		res.version_log.emplace_back(item.Copy());
	}
	res.schemas.reserve(schemas.size());
	for (auto &item : schemas) {
		res.schemas.emplace_back(item.Copy());
	}
	if (properties.has_value()) {
		res.properties.emplace();
		for (auto &entry : (*properties)) {
			(*res.properties).emplace(entry.first, entry.second);
		}
	}
	return res;
}

string ViewMetadata::TryFromJSON(JSONValue obj) {
	string error;
	auto view_uuid_val = obj.GetMember("view-uuid");
	if (!view_uuid_val.IsValid()) {
		return "ViewMetadata required property 'view-uuid' is missing";
	} else {
		if (json_utils::IsString(view_uuid_val)) {
			view_uuid = json_utils::GetString(view_uuid_val);
		} else {
			return StringUtil::Format("ViewMetadata property 'view_uuid' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(view_uuid_val).c_str());
		}
	}
	auto format_version_val = obj.GetMember("format-version");
	if (!format_version_val.IsValid()) {
		return "ViewMetadata required property 'format-version' is missing";
	} else {
		if (json_utils::IsInteger(format_version_val)) {
			format_version = json_utils::GetSignedInteger(format_version_val);
		} else {
			return StringUtil::Format(
			    "ViewMetadata property 'format_version' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(format_version_val).c_str());
		}
	}
	auto location_val = obj.GetMember("location");
	if (!location_val.IsValid()) {
		return "ViewMetadata required property 'location' is missing";
	} else {
		if (json_utils::IsString(location_val)) {
			location = json_utils::GetString(location_val);
		} else {
			return StringUtil::Format("ViewMetadata property 'location' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(location_val).c_str());
		}
	}
	auto current_version_id_val = obj.GetMember("current-version-id");
	if (!current_version_id_val.IsValid()) {
		return "ViewMetadata required property 'current-version-id' is missing";
	} else {
		if (json_utils::IsInteger(current_version_id_val)) {
			current_version_id = json_utils::GetSignedInteger(current_version_id_val);
		} else {
			return StringUtil::Format(
			    "ViewMetadata property 'current_version_id' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(current_version_id_val).c_str());
		}
	}
	auto versions_val = obj.GetMember("versions");
	if (!versions_val.IsValid()) {
		return "ViewMetadata required property 'versions' is missing";
	} else {
		if (versions_val.IsArray()) {
			versions_val.IterateArray([&](JSONValue versions_item_val) {
				if (!error.empty()) {
					return;
				}
				ViewVersion versions_item;
				error = versions_item.TryFromJSON(versions_item_val);
				if (!error.empty()) {
					return;
				}
				versions.emplace_back(std::move(versions_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("ViewMetadata property 'versions' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(versions_val).c_str());
		}
	}
	auto version_log_val = obj.GetMember("version-log");
	if (!version_log_val.IsValid()) {
		return "ViewMetadata required property 'version-log' is missing";
	} else {
		if (version_log_val.IsArray()) {
			version_log_val.IterateArray([&](JSONValue version_log_item_val) {
				if (!error.empty()) {
					return;
				}
				ViewHistoryEntry version_log_item;
				error = version_log_item.TryFromJSON(version_log_item_val);
				if (!error.empty()) {
					return;
				}
				version_log.emplace_back(std::move(version_log_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("ViewMetadata property 'version_log' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(version_log_val).c_str());
		}
	}
	auto schemas_val = obj.GetMember("schemas");
	if (!schemas_val.IsValid()) {
		return "ViewMetadata required property 'schemas' is missing";
	} else {
		if (schemas_val.IsArray()) {
			schemas_val.IterateArray([&](JSONValue schemas_item_val) {
				if (!error.empty()) {
					return;
				}
				Schema schemas_item;
				error = schemas_item.TryFromJSON(schemas_item_val);
				if (!error.empty()) {
					return;
				}
				schemas.emplace_back(std::move(schemas_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("ViewMetadata property 'schemas' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(schemas_val).c_str());
		}
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
					error = StringUtil::Format("ViewMetadata property 'tmp' is not of type 'string', found %s instead",
					                           json_utils::GetTypeDescription(val).c_str());
					return;
				}
				properties_tmp.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "ViewMetadata property 'properties_tmp' is not of type 'object'";
		}
		properties = std::move(properties_tmp);
	}
	return "";
}

void ViewMetadata::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: view-uuid
	auto view_uuid_json = writer.CreateString(view_uuid);
	obj.Add("view-uuid", view_uuid_json);

	// Serialize: format-version
	auto format_version_json = writer.CreateSignedInteger(format_version);
	obj.Add("format-version", format_version_json);

	// Serialize: location
	auto location_json = writer.CreateString(location);
	obj.Add("location", location_json);

	// Serialize: current-version-id
	auto current_version_id_json = writer.CreateSignedInteger(current_version_id);
	obj.Add("current-version-id", current_version_id_json);

	// Serialize: versions
	auto versions_json = writer.CreateArray();
	for (const auto &versions_json_item : versions) {
		auto versions_json_item_json = versions_json_item.ToJSON(writer);
		versions_json.Append(versions_json_item_json);
	}
	obj.Add("versions", versions_json);

	// Serialize: version-log
	auto version_log_json = writer.CreateArray();
	for (const auto &version_log_json_item : version_log) {
		auto version_log_json_item_json = version_log_json_item.ToJSON(writer);
		version_log_json.Append(version_log_json_item_json);
	}
	obj.Add("version-log", version_log_json);

	// Serialize: schemas
	auto schemas_json = writer.CreateArray();
	for (const auto &schemas_json_item : schemas) {
		auto schemas_json_item_json = schemas_json_item.ToJSON(writer);
		schemas_json.Append(schemas_json_item_json);
	}
	obj.Add("schemas", schemas_json);

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

JSONMutableValue ViewMetadata::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
