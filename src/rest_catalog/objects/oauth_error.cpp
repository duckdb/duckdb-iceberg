
#include "rest_catalog/objects/oauth_error.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

OAuthError::OAuthError() {
}

OAuthError OAuthError::FromJSON(JSONValue obj) {
	OAuthError res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

OAuthError OAuthError::Copy() const {
	OAuthError res;
	res._error = _error;
	if (error_description.has_value()) {
		res.error_description.emplace();
		(*res.error_description) = (*error_description);
	}
	if (error_uri.has_value()) {
		res.error_uri.emplace();
		(*res.error_uri) = (*error_uri);
	}
	return res;
}

string OAuthError::TryFromJSON(JSONValue obj) {
	string error;
	auto _error_val = obj.GetMember("error");
	if (!_error_val.IsValid()) {
		return "OAuthError required property 'error' is missing";
	} else {
		if (json_utils::IsString(_error_val)) {
			_error = json_utils::GetString(_error_val);
		} else {
			return StringUtil::Format("OAuthError property '_error' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(_error_val).c_str());
		}
	}
	auto error_description_val = obj.GetMember("error_description");
	if (error_description_val.IsValid()) {
		string error_description_tmp;
		if (json_utils::IsString(error_description_val)) {
			error_description_tmp = json_utils::GetString(error_description_val);
		} else {
			return StringUtil::Format(
			    "OAuthError property 'error_description_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(error_description_val).c_str());
		}
		error_description = std::move(error_description_tmp);
	}
	auto error_uri_val = obj.GetMember("error_uri");
	if (error_uri_val.IsValid()) {
		string error_uri_tmp;
		if (json_utils::IsString(error_uri_val)) {
			error_uri_tmp = json_utils::GetString(error_uri_val);
		} else {
			return StringUtil::Format("OAuthError property 'error_uri_tmp' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(error_uri_val).c_str());
		}
		error_uri = std::move(error_uri_tmp);
	}
	return "";
}

void OAuthError::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: error
	auto _error_json = writer.CreateString(_error);
	obj.Add("error", _error_json);

	// Serialize: error_description
	if (error_description.has_value()) {
		auto &error_description_value = *error_description;
		auto error_description_json = writer.CreateString(error_description_value);
		obj.Add("error_description", error_description_json);
	}

	// Serialize: error_uri
	if (error_uri.has_value()) {
		auto &error_uri_value = *error_uri;
		auto error_uri_json = writer.CreateString(error_uri_value);
		obj.Add("error_uri", error_uri_json);
	}
}

JSONMutableValue OAuthError::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
