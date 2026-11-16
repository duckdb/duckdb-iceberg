
#include "rest_catalog/objects/remote_sign_request.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

RemoteSignRequest::RemoteSignRequest() {
}

RemoteSignRequest RemoteSignRequest::FromJSON(JSONValue obj) {
	RemoteSignRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

RemoteSignRequest RemoteSignRequest::Copy() const {
	RemoteSignRequest res;
	res.region = region;
	res.uri = uri;
	res.method = method;
	res.headers = headers.Copy();
	if (properties.has_value()) {
		res.properties.emplace();
		for (auto &entry : (*properties)) {
			(*res.properties).emplace(entry.first, entry.second);
		}
	}
	if (body.has_value()) {
		res.body.emplace();
		(*res.body) = (*body);
	}
	if (provider.has_value()) {
		res.provider.emplace();
		(*res.provider) = (*provider);
	}
	return res;
}

string RemoteSignRequest::TryFromJSON(JSONValue obj) {
	string error;
	auto region_val = obj.GetMember("region");
	if (!region_val.IsValid()) {
		return "RemoteSignRequest required property 'region' is missing";
	} else {
		if (json_utils::IsString(region_val)) {
			region = json_utils::GetString(region_val);
		} else {
			return StringUtil::Format("RemoteSignRequest property 'region' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(region_val).c_str());
		}
	}
	auto uri_val = obj.GetMember("uri");
	if (!uri_val.IsValid()) {
		return "RemoteSignRequest required property 'uri' is missing";
	} else {
		if (json_utils::IsString(uri_val)) {
			uri = json_utils::GetString(uri_val);
		} else {
			return StringUtil::Format("RemoteSignRequest property 'uri' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(uri_val).c_str());
		}
	}
	auto method_val = obj.GetMember("method");
	if (!method_val.IsValid()) {
		return "RemoteSignRequest required property 'method' is missing";
	} else {
		if (json_utils::IsString(method_val)) {
			method = json_utils::GetString(method_val);
		} else {
			return StringUtil::Format("RemoteSignRequest property 'method' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(method_val).c_str());
		}
	}
	auto headers_val = obj.GetMember("headers");
	if (!headers_val.IsValid()) {
		return "RemoteSignRequest required property 'headers' is missing";
	} else {
		error = headers.TryFromJSON(headers_val);
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
					error =
					    StringUtil::Format("RemoteSignRequest property 'tmp' is not of type 'string', found %s instead",
					                       json_utils::GetTypeDescription(val).c_str());
					return;
				}
				properties_tmp.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "RemoteSignRequest property 'properties_tmp' is not of type 'object'";
		}
		properties = std::move(properties_tmp);
	}
	auto body_val = obj.GetMember("body");
	if (body_val.IsValid()) {
		string body_tmp;
		if (json_utils::IsString(body_val)) {
			body_tmp = json_utils::GetString(body_val);
		} else {
			return StringUtil::Format("RemoteSignRequest property 'body_tmp' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(body_val).c_str());
		}
		body = std::move(body_tmp);
	}
	auto provider_val = obj.GetMember("provider");
	if (provider_val.IsValid()) {
		string provider_tmp;
		if (json_utils::IsString(provider_val)) {
			provider_tmp = json_utils::GetString(provider_val);
		} else {
			return StringUtil::Format(
			    "RemoteSignRequest property 'provider_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(provider_val).c_str());
		}
		provider = std::move(provider_tmp);
	}
	return "";
}

void RemoteSignRequest::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: region
	auto region_json = writer.CreateString(region);
	obj.Add("region", region_json);

	// Serialize: uri
	auto uri_json = writer.CreateString(uri);
	obj.Add("uri", uri_json);

	// Serialize: method
	auto method_json = writer.CreateString(method);
	obj.Add("method", method_json);

	// Serialize: headers
	auto headers_json = headers.ToJSON(writer);
	obj.Add("headers", headers_json);

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

	// Serialize: body
	if (body.has_value()) {
		auto &body_value = *body;
		auto body_json = writer.CreateString(body_value);
		obj.Add("body", body_json);
	}

	// Serialize: provider
	if (provider.has_value()) {
		auto &provider_value = *provider;
		auto provider_json = writer.CreateString(provider_value);
		obj.Add("provider", provider_json);
	}
}

JSONMutableValue RemoteSignRequest::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
