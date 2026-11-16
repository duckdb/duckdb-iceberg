
#include "rest_catalog/objects/oauth_token_response.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

OAuthTokenResponse::OAuthTokenResponse() {
}

OAuthTokenResponse OAuthTokenResponse::FromJSON(JSONValue obj) {
	OAuthTokenResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

OAuthTokenResponse OAuthTokenResponse::Copy() const {
	OAuthTokenResponse res;
	res.access_token = access_token;
	res.token_type = token_type;
	if (expires_in.has_value()) {
		res.expires_in.emplace();
		(*res.expires_in) = (*expires_in);
	}
	if (issued_token_type.has_value()) {
		res.issued_token_type.emplace();
		(*res.issued_token_type) = (*issued_token_type).Copy();
	}
	if (refresh_token.has_value()) {
		res.refresh_token.emplace();
		(*res.refresh_token) = (*refresh_token);
	}
	if (scope.has_value()) {
		res.scope.emplace();
		(*res.scope) = (*scope);
	}
	return res;
}

string OAuthTokenResponse::TryFromJSON(JSONValue obj) {
	string error;
	auto access_token_val = obj.GetMember("access_token");
	if (!access_token_val.IsValid()) {
		return "OAuthTokenResponse required property 'access_token' is missing";
	} else {
		if (json_utils::IsString(access_token_val)) {
			access_token = json_utils::GetString(access_token_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenResponse property 'access_token' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(access_token_val).c_str());
		}
	}
	auto token_type_val = obj.GetMember("token_type");
	if (!token_type_val.IsValid()) {
		return "OAuthTokenResponse required property 'token_type' is missing";
	} else {
		if (json_utils::IsString(token_type_val)) {
			token_type = json_utils::GetString(token_type_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenResponse property 'token_type' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(token_type_val).c_str());
		}
	}
	auto expires_in_val = obj.GetMember("expires_in");
	if (expires_in_val.IsValid()) {
		int32_t expires_in_tmp;
		if (json_utils::IsInteger(expires_in_val)) {
			expires_in_tmp = json_utils::GetSignedInteger(expires_in_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenResponse property 'expires_in_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(expires_in_val).c_str());
		}
		expires_in = std::move(expires_in_tmp);
	}
	auto issued_token_type_val = obj.GetMember("issued_token_type");
	if (issued_token_type_val.IsValid()) {
		TokenType issued_token_type_tmp;
		error = issued_token_type_tmp.TryFromJSON(issued_token_type_val);
		if (!error.empty()) {
			return error;
		}
		issued_token_type = std::move(issued_token_type_tmp);
	}
	auto refresh_token_val = obj.GetMember("refresh_token");
	if (refresh_token_val.IsValid()) {
		string refresh_token_tmp;
		if (json_utils::IsString(refresh_token_val)) {
			refresh_token_tmp = json_utils::GetString(refresh_token_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenResponse property 'refresh_token_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(refresh_token_val).c_str());
		}
		refresh_token = std::move(refresh_token_tmp);
	}
	auto scope_val = obj.GetMember("scope");
	if (scope_val.IsValid()) {
		string scope_tmp;
		if (json_utils::IsString(scope_val)) {
			scope_tmp = json_utils::GetString(scope_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenResponse property 'scope_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(scope_val).c_str());
		}
		scope = std::move(scope_tmp);
	}
	return "";
}

void OAuthTokenResponse::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: access_token
	auto access_token_json = writer.CreateString(access_token);
	obj.Add("access_token", access_token_json);

	// Serialize: token_type
	auto token_type_json = writer.CreateString(token_type);
	obj.Add("token_type", token_type_json);

	// Serialize: expires_in
	if (expires_in.has_value()) {
		auto &expires_in_value = *expires_in;
		auto expires_in_json = writer.CreateSignedInteger(expires_in_value);
		obj.Add("expires_in", expires_in_json);
	}

	// Serialize: issued_token_type
	if (issued_token_type.has_value()) {
		auto &issued_token_type_value = *issued_token_type;
		auto issued_token_type_json = issued_token_type_value.ToJSON(writer);
		obj.Add("issued_token_type", issued_token_type_json);
	}

	// Serialize: refresh_token
	if (refresh_token.has_value()) {
		auto &refresh_token_value = *refresh_token;
		auto refresh_token_json = writer.CreateString(refresh_token_value);
		obj.Add("refresh_token", refresh_token_json);
	}

	// Serialize: scope
	if (scope.has_value()) {
		auto &scope_value = *scope;
		auto scope_json = writer.CreateString(scope_value);
		obj.Add("scope", scope_json);
	}
}

JSONMutableValue OAuthTokenResponse::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
