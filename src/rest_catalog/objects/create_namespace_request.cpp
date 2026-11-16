
#include "rest_catalog/objects/create_namespace_request.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

CreateNamespaceRequest::CreateNamespaceRequest() {
}

CreateNamespaceRequest CreateNamespaceRequest::FromJSON(JSONValue obj) {
	CreateNamespaceRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CreateNamespaceRequest CreateNamespaceRequest::Copy() const {
	CreateNamespaceRequest res;
	res._namespace = _namespace.Copy();
	if (properties.has_value()) {
		res.properties.emplace();
		for (auto &entry : (*properties)) {
			(*res.properties).emplace(entry.first, entry.second);
		}
	}
	return res;
}

string CreateNamespaceRequest::TryFromJSON(JSONValue obj) {
	string error;
	auto _namespace_val = obj.GetMember("namespace");
	if (!_namespace_val.IsValid()) {
		return "CreateNamespaceRequest required property 'namespace' is missing";
	} else {
		error = _namespace.TryFromJSON(_namespace_val);
		if (!error.empty()) {
			return error;
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
					error = StringUtil::Format(
					    "CreateNamespaceRequest property 'tmp' is not of type 'string', found %s instead",
					    json_utils::GetTypeDescription(val).c_str());
					return;
				}
				properties_tmp.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "CreateNamespaceRequest property 'properties_tmp' is not of type 'object'";
		}
		properties = std::move(properties_tmp);
	}
	return "";
}

void CreateNamespaceRequest::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: namespace
	auto _namespace_json = _namespace.ToJSON(writer);
	obj.Add("namespace", _namespace_json);

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

JSONMutableValue CreateNamespaceRequest::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
