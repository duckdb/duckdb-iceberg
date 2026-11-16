
#include "rest_catalog/objects/view_version.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ViewVersion::ViewVersion() {
}

ViewVersion ViewVersion::FromJSON(JSONValue obj) {
	ViewVersion res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ViewVersion ViewVersion::Copy() const {
	ViewVersion res;
	res.version_id = version_id;
	res.timestamp_ms = timestamp_ms;
	res.schema_id = schema_id;
	for (auto &entry : summary) {
		res.summary.emplace(entry.first, entry.second);
	}
	res.representations.reserve(representations.size());
	for (auto &item : representations) {
		res.representations.emplace_back(item.Copy());
	}
	res.default_namespace = default_namespace.Copy();
	if (default_catalog.has_value()) {
		res.default_catalog.emplace();
		(*res.default_catalog) = (*default_catalog);
	}
	return res;
}

string ViewVersion::TryFromJSON(JSONValue obj) {
	string error;
	auto version_id_val = obj.GetMember("version-id");
	if (!version_id_val.IsValid()) {
		return "ViewVersion required property 'version-id' is missing";
	} else {
		if (json_utils::IsInteger(version_id_val)) {
			version_id = json_utils::GetSignedInteger(version_id_val);
		} else {
			return StringUtil::Format("ViewVersion property 'version_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(version_id_val).c_str());
		}
	}
	auto timestamp_ms_val = obj.GetMember("timestamp-ms");
	if (!timestamp_ms_val.IsValid()) {
		return "ViewVersion required property 'timestamp-ms' is missing";
	} else {
		if (json_utils::IsInteger(timestamp_ms_val)) {
			timestamp_ms = json_utils::GetSignedInteger(timestamp_ms_val);
		} else if (json_utils::IsUnsignedInteger(timestamp_ms_val)) {
			timestamp_ms = json_utils::GetUnsignedInteger(timestamp_ms_val);
		} else {
			return StringUtil::Format("ViewVersion property 'timestamp_ms' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(timestamp_ms_val).c_str());
		}
	}
	auto schema_id_val = obj.GetMember("schema-id");
	if (!schema_id_val.IsValid()) {
		return "ViewVersion required property 'schema-id' is missing";
	} else {
		if (json_utils::IsInteger(schema_id_val)) {
			schema_id = json_utils::GetSignedInteger(schema_id_val);
		} else {
			return StringUtil::Format("ViewVersion property 'schema_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(schema_id_val).c_str());
		}
	}
	auto summary_val = obj.GetMember("summary");
	if (!summary_val.IsValid()) {
		return "ViewVersion required property 'summary' is missing";
	} else {
		if (summary_val.IsObject()) {
			summary_val.IterateObject([&](const string &key_str, JSONValue val) {
				if (!error.empty()) {
					return;
				}
				string tmp;
				if (json_utils::IsString(val)) {
					tmp = json_utils::GetString(val);
				} else {
					error = StringUtil::Format("ViewVersion property 'tmp' is not of type 'string', found %s instead",
					                           json_utils::GetTypeDescription(val).c_str());
					return;
				}
				summary.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "ViewVersion property 'summary' is not of type 'object'";
		}
	}
	auto representations_val = obj.GetMember("representations");
	if (!representations_val.IsValid()) {
		return "ViewVersion required property 'representations' is missing";
	} else {
		if (representations_val.IsArray()) {
			representations_val.IterateArray([&](JSONValue representations_item_val) {
				if (!error.empty()) {
					return;
				}
				ViewRepresentation representations_item;
				error = representations_item.TryFromJSON(representations_item_val);
				if (!error.empty()) {
					return;
				}
				representations.emplace_back(std::move(representations_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("ViewVersion property 'representations' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(representations_val).c_str());
		}
	}
	auto default_namespace_val = obj.GetMember("default-namespace");
	if (!default_namespace_val.IsValid()) {
		return "ViewVersion required property 'default-namespace' is missing";
	} else {
		error = default_namespace.TryFromJSON(default_namespace_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto default_catalog_val = obj.GetMember("default-catalog");
	if (default_catalog_val.IsValid()) {
		string default_catalog_tmp;
		if (json_utils::IsString(default_catalog_val)) {
			default_catalog_tmp = json_utils::GetString(default_catalog_val);
		} else {
			return StringUtil::Format(
			    "ViewVersion property 'default_catalog_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(default_catalog_val).c_str());
		}
		default_catalog = std::move(default_catalog_tmp);
	}
	return "";
}

void ViewVersion::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: version-id
	auto version_id_json = writer.CreateSignedInteger(version_id);
	obj.Add("version-id", version_id_json);

	// Serialize: timestamp-ms
	auto timestamp_ms_json = writer.CreateSignedInteger(timestamp_ms);
	obj.Add("timestamp-ms", timestamp_ms_json);

	// Serialize: schema-id
	auto schema_id_json = writer.CreateSignedInteger(schema_id);
	obj.Add("schema-id", schema_id_json);

	// Serialize: summary
	auto summary_json = writer.CreateObject();
	for (const auto &[summary_json_key, summary_json_value] : summary) {
		auto summary_json_value_json = writer.CreateString(summary_json_value);
		summary_json.Add(summary_json_key, summary_json_value_json);
	}
	obj.Add("summary", summary_json);

	// Serialize: representations
	auto representations_json = writer.CreateArray();
	for (const auto &representations_json_item : representations) {
		auto representations_json_item_json = representations_json_item.ToJSON(writer);
		representations_json.Append(representations_json_item_json);
	}
	obj.Add("representations", representations_json);

	// Serialize: default-namespace
	auto default_namespace_json = default_namespace.ToJSON(writer);
	obj.Add("default-namespace", default_namespace_json);

	// Serialize: default-catalog
	if (default_catalog.has_value()) {
		auto &default_catalog_value = *default_catalog;
		auto default_catalog_json = writer.CreateString(default_catalog_value);
		obj.Add("default-catalog", default_catalog_json);
	}
}

JSONMutableValue ViewVersion::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
