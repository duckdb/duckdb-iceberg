
#include "rest_catalog/objects/create_view_request.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

CreateViewRequest::CreateViewRequest() {
}

CreateViewRequest CreateViewRequest::FromJSON(JSONValue obj) {
	CreateViewRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CreateViewRequest CreateViewRequest::Copy() const {
	CreateViewRequest res;
	res.name = name;
	res.schema = schema.Copy();
	res.view_version = view_version.Copy();
	for (auto &entry : properties) {
		res.properties.emplace(entry.first, entry.second);
	}
	if (location.has_value()) {
		res.location.emplace();
		(*res.location) = (*location);
	}
	return res;
}

string CreateViewRequest::TryFromJSON(JSONValue obj) {
	string error;
	auto name_val = obj.GetMember("name");
	if (!name_val.IsValid()) {
		return "CreateViewRequest required property 'name' is missing";
	} else {
		if (json_utils::IsString(name_val)) {
			name = json_utils::GetString(name_val);
		} else {
			return StringUtil::Format("CreateViewRequest property 'name' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(name_val).c_str());
		}
	}
	auto schema_val = obj.GetMember("schema");
	if (!schema_val.IsValid()) {
		return "CreateViewRequest required property 'schema' is missing";
	} else {
		error = schema.TryFromJSON(schema_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto view_version_val = obj.GetMember("view-version");
	if (!view_version_val.IsValid()) {
		return "CreateViewRequest required property 'view-version' is missing";
	} else {
		error = view_version.TryFromJSON(view_version_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto properties_val = obj.GetMember("properties");
	if (!properties_val.IsValid()) {
		return "CreateViewRequest required property 'properties' is missing";
	} else {
		if (properties_val.IsObject()) {
			properties_val.IterateObject([&](const string &key_str, JSONValue val) {
				if (!error.empty()) {
					return;
				}
				string tmp;
				if (json_utils::IsString(val)) {
					tmp = json_utils::GetString(val);
				} else {
					error =
					    StringUtil::Format("CreateViewRequest property 'tmp' is not of type 'string', found %s instead",
					                       json_utils::GetTypeDescription(val).c_str());
					return;
				}
				properties.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "CreateViewRequest property 'properties' is not of type 'object'";
		}
	}
	auto location_val = obj.GetMember("location");
	if (location_val.IsValid()) {
		string location_tmp;
		if (json_utils::IsString(location_val)) {
			location_tmp = json_utils::GetString(location_val);
		} else {
			return StringUtil::Format(
			    "CreateViewRequest property 'location_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(location_val).c_str());
		}
		location = std::move(location_tmp);
	}
	return "";
}

void CreateViewRequest::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: name
	auto name_json = writer.CreateString(name);
	obj.Add("name", name_json);

	// Serialize: schema
	auto schema_json = schema.ToJSON(writer);
	obj.Add("schema", schema_json);

	// Serialize: view-version
	auto view_version_json = view_version.ToJSON(writer);
	obj.Add("view-version", view_version_json);

	// Serialize: properties
	auto properties_json = writer.CreateObject();
	for (const auto &[properties_json_key, properties_json_value] : properties) {
		auto properties_json_value_json = writer.CreateString(properties_json_value);
		properties_json.Add(properties_json_key, properties_json_value_json);
	}
	obj.Add("properties", properties_json);

	// Serialize: location
	if (location.has_value()) {
		auto &location_value = *location;
		auto location_json = writer.CreateString(location_value);
		obj.Add("location", location_json);
	}
}

JSONMutableValue CreateViewRequest::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
