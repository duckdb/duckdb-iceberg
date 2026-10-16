
#include "rest_catalog/objects/remote_signing_config.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

RemoteSigningConfig::RemoteSigningConfig() {
}

RemoteSigningConfig RemoteSigningConfig::FromJSON(JSONValue obj) {
	RemoteSigningConfig res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

RemoteSigningConfig RemoteSigningConfig::Copy() const {
	RemoteSigningConfig res;
	if (properties.has_value()) {
		res.properties.emplace();
		for (auto &entry : (*properties)) {
			(*res.properties).emplace(entry.first, entry.second);
		}
	}
	if (headers.has_value()) {
		res.headers.emplace();
		(*res.headers) = (*headers).Copy();
	}
	return res;
}

string RemoteSigningConfig::TryFromJSON(JSONValue obj) {
	string error;
	auto properties_val = obj.GetMember("properties");
	if (properties_val.IsValid()) {
		case_insensitive_map_t<string> properties_tmp;
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
					    "RemoteSigningConfig property 'tmp' is not of type 'string', found %s instead",
					    json_utils::GetTypeDescription(val).c_str());
					return;
				}
				properties_tmp.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "RemoteSigningConfig property 'properties_tmp' is not of type 'object'";
		}
		properties = std::move(properties_tmp);
	}
	auto headers_val = obj.GetMember("headers");
	if (headers_val.IsValid()) {
		MultiValuedMap headers_tmp;
		error = headers_tmp.TryFromJSON(headers_val);
		if (!error.empty()) {
			return error;
		}
		headers = std::move(headers_tmp);
	}
	return "";
}

void RemoteSigningConfig::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
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

	// Serialize: headers
	if (headers.has_value()) {
		auto &headers_value = *headers;
		auto headers_json = headers_value.ToJSON(writer);
		obj.Add("headers", headers_json);
	}
}

JSONMutableValue RemoteSigningConfig::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
