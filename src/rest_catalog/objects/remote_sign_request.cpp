
#include "rest_catalog/objects/remote_sign_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

RemoteSignRequest::RemoteSignRequest() {
}

RemoteSignRequest RemoteSignRequest::FromJSON(yyjson_val *obj) {
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

string RemoteSignRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto region_val = yyjson_obj_get(obj, "region");
	if (!region_val) {
		return "RemoteSignRequest required property 'region' is missing";
	} else {
		if (yyjson_is_str(region_val)) {
			region = yyjson_get_str(region_val);
		} else {
			return StringUtil::Format("RemoteSignRequest property 'region' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(region_val));
		}
	}
	auto uri_val = yyjson_obj_get(obj, "uri");
	if (!uri_val) {
		return "RemoteSignRequest required property 'uri' is missing";
	} else {
		if (yyjson_is_str(uri_val)) {
			uri = yyjson_get_str(uri_val);
		} else {
			return StringUtil::Format("RemoteSignRequest property 'uri' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(uri_val));
		}
	}
	auto method_val = yyjson_obj_get(obj, "method");
	if (!method_val) {
		return "RemoteSignRequest required property 'method' is missing";
	} else {
		if (yyjson_is_str(method_val)) {
			method = yyjson_get_str(method_val);
		} else {
			return StringUtil::Format("RemoteSignRequest property 'method' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(method_val));
		}
	}
	auto headers_val = yyjson_obj_get(obj, "headers");
	if (!headers_val) {
		return "RemoteSignRequest required property 'headers' is missing";
	} else {
		error = headers.TryFromJSON(headers_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		case_insensitive_map_t<string> properties_tmp;
		if (yyjson_is_obj(properties_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(properties_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "RemoteSignRequest property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				properties_tmp.emplace(key_str, std::move(tmp));
			}
		} else {
			return "RemoteSignRequest property 'properties_tmp' is not of type 'object'";
		}
		properties = std::move(properties_tmp);
	}
	auto body_val = yyjson_obj_get(obj, "body");
	if (body_val) {
		string body_tmp;
		if (yyjson_is_str(body_val)) {
			body_tmp = yyjson_get_str(body_val);
		} else {
			return StringUtil::Format(
			    "RemoteSignRequest property 'body_tmp' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(body_val));
		}
		body = std::move(body_tmp);
	}
	auto provider_val = yyjson_obj_get(obj, "provider");
	if (provider_val) {
		string provider_tmp;
		if (yyjson_is_str(provider_val)) {
			provider_tmp = yyjson_get_str(provider_val);
		} else {
			return StringUtil::Format(
			    "RemoteSignRequest property 'provider_tmp' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(provider_val));
		}
		provider = std::move(provider_tmp);
	}
	return "";
}

void RemoteSignRequest::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: region
	yyjson_mut_obj_add_strcpy(doc, obj, "region", region.c_str());

	// Serialize: uri
	yyjson_mut_obj_add_strcpy(doc, obj, "uri", uri.c_str());

	// Serialize: method
	yyjson_mut_obj_add_strcpy(doc, obj, "method", method.c_str());

	// Serialize: headers
	yyjson_mut_val *headers_val = headers.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "headers", headers_val);

	// Serialize: properties
	if (properties.has_value()) {
		auto &properties_value = *properties;
		yyjson_mut_val *properties_value_obj = yyjson_mut_obj(doc);
		for (const auto &it : properties_value) {
			auto &key = it.first;
			auto &value = it.second;
			auto key_ptr = unsafe_yyjson_mut_strncpy(doc, key.c_str(), strlen(key.c_str()));
			yyjson_mut_obj_add_strcpy(doc, properties_value_obj, key_ptr, value.c_str());
		}
		yyjson_mut_obj_add_val(doc, obj, "properties", properties_value_obj);
	}

	// Serialize: body
	if (body.has_value()) {
		auto &body_value = *body;
		yyjson_mut_obj_add_strcpy(doc, obj, "body", body_value.c_str());
	}

	// Serialize: provider
	if (provider.has_value()) {
		auto &provider_value = *provider;
		yyjson_mut_obj_add_strcpy(doc, obj, "provider", provider_value.c_str());
	}
}

yyjson_mut_val *RemoteSignRequest::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
