
#include "rest_catalog/objects/oauth_client_credentials_request.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

OAuthClientCredentialsRequest::OAuthClientCredentialsRequest() {
}

OAuthClientCredentialsRequest OAuthClientCredentialsRequest::FromJSON(JSONValue obj) {
	OAuthClientCredentialsRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

OAuthClientCredentialsRequest OAuthClientCredentialsRequest::Copy() const {
	OAuthClientCredentialsRequest res;
	res.grant_type = grant_type;
	res.client_id = client_id;
	res.client_secret = client_secret;
	if (scope.has_value()) {
		res.scope.emplace();
		(*res.scope) = (*scope);
	}
	return res;
}

string OAuthClientCredentialsRequest::TryFromJSON(JSONValue obj) {
	string error;
	auto grant_type_val = obj.GetMember("grant_type");
	if (!grant_type_val.IsValid()) {
		return "OAuthClientCredentialsRequest required property 'grant_type' is missing";
	} else {
		if (json_utils::IsString(grant_type_val)) {
			grant_type = json_utils::GetString(grant_type_val);
		} else {
			return StringUtil::Format(
			    "OAuthClientCredentialsRequest property 'grant_type' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(grant_type_val).c_str());
		}
	}
	auto client_id_val = obj.GetMember("client_id");
	if (!client_id_val.IsValid()) {
		return "OAuthClientCredentialsRequest required property 'client_id' is missing";
	} else {
		if (json_utils::IsString(client_id_val)) {
			client_id = json_utils::GetString(client_id_val);
		} else {
			return StringUtil::Format(
			    "OAuthClientCredentialsRequest property 'client_id' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(client_id_val).c_str());
		}
	}
	auto client_secret_val = obj.GetMember("client_secret");
	if (!client_secret_val.IsValid()) {
		return "OAuthClientCredentialsRequest required property 'client_secret' is missing";
	} else {
		if (json_utils::IsString(client_secret_val)) {
			client_secret = json_utils::GetString(client_secret_val);
		} else {
			return StringUtil::Format(
			    "OAuthClientCredentialsRequest property 'client_secret' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(client_secret_val).c_str());
		}
	}
	auto scope_val = obj.GetMember("scope");
	if (scope_val.IsValid()) {
		string scope_tmp;
		if (json_utils::IsString(scope_val)) {
			scope_tmp = json_utils::GetString(scope_val);
		} else {
			return StringUtil::Format(
			    "OAuthClientCredentialsRequest property 'scope_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(scope_val).c_str());
		}
		scope = std::move(scope_tmp);
	}
	return "";
}

void OAuthClientCredentialsRequest::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: grant_type
	auto grant_type_json = writer.CreateString(grant_type);
	obj.Add("grant_type", grant_type_json);

	// Serialize: client_id
	auto client_id_json = writer.CreateString(client_id);
	obj.Add("client_id", client_id_json);

	// Serialize: client_secret
	auto client_secret_json = writer.CreateString(client_secret);
	obj.Add("client_secret", client_secret_json);

	// Serialize: scope
	if (scope.has_value()) {
		auto &scope_value = *scope;
		auto scope_json = writer.CreateString(scope_value);
		obj.Add("scope", scope_json);
	}
}

JSONMutableValue OAuthClientCredentialsRequest::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
