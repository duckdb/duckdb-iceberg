
#include "rest_catalog/objects/oauth_token_exchange_request.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

OAuthTokenExchangeRequest::OAuthTokenExchangeRequest() {
}

OAuthTokenExchangeRequest OAuthTokenExchangeRequest::FromJSON(JSONValue obj) {
	OAuthTokenExchangeRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

OAuthTokenExchangeRequest OAuthTokenExchangeRequest::Copy() const {
	OAuthTokenExchangeRequest res;
	res.grant_type = grant_type;
	res.subject_token = subject_token;
	res.subject_token_type = subject_token_type.Copy();
	if (scope.has_value()) {
		res.scope.emplace();
		(*res.scope) = (*scope);
	}
	if (requested_token_type.has_value()) {
		res.requested_token_type.emplace();
		(*res.requested_token_type) = (*requested_token_type).Copy();
	}
	if (actor_token.has_value()) {
		res.actor_token.emplace();
		(*res.actor_token) = (*actor_token);
	}
	if (actor_token_type.has_value()) {
		res.actor_token_type.emplace();
		(*res.actor_token_type) = (*actor_token_type).Copy();
	}
	return res;
}

string OAuthTokenExchangeRequest::TryFromJSON(JSONValue obj) {
	string error;
	auto grant_type_val = obj.GetMember("grant_type");
	if (!grant_type_val.IsValid()) {
		return "OAuthTokenExchangeRequest required property 'grant_type' is missing";
	} else {
		if (json_utils::IsString(grant_type_val)) {
			grant_type = json_utils::GetString(grant_type_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenExchangeRequest property 'grant_type' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(grant_type_val).c_str());
		}
	}
	auto subject_token_val = obj.GetMember("subject_token");
	if (!subject_token_val.IsValid()) {
		return "OAuthTokenExchangeRequest required property 'subject_token' is missing";
	} else {
		if (json_utils::IsString(subject_token_val)) {
			subject_token = json_utils::GetString(subject_token_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenExchangeRequest property 'subject_token' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(subject_token_val).c_str());
		}
	}
	auto subject_token_type_val = obj.GetMember("subject_token_type");
	if (!subject_token_type_val.IsValid()) {
		return "OAuthTokenExchangeRequest required property 'subject_token_type' is missing";
	} else {
		error = subject_token_type.TryFromJSON(subject_token_type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto scope_val = obj.GetMember("scope");
	if (scope_val.IsValid()) {
		string scope_tmp;
		if (json_utils::IsString(scope_val)) {
			scope_tmp = json_utils::GetString(scope_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenExchangeRequest property 'scope_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(scope_val).c_str());
		}
		scope = std::move(scope_tmp);
	}
	auto requested_token_type_val = obj.GetMember("requested_token_type");
	if (requested_token_type_val.IsValid()) {
		TokenType requested_token_type_tmp;
		error = requested_token_type_tmp.TryFromJSON(requested_token_type_val);
		if (!error.empty()) {
			return error;
		}
		requested_token_type = std::move(requested_token_type_tmp);
	}
	auto actor_token_val = obj.GetMember("actor_token");
	if (actor_token_val.IsValid()) {
		string actor_token_tmp;
		if (json_utils::IsString(actor_token_val)) {
			actor_token_tmp = json_utils::GetString(actor_token_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenExchangeRequest property 'actor_token_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(actor_token_val).c_str());
		}
		actor_token = std::move(actor_token_tmp);
	}
	auto actor_token_type_val = obj.GetMember("actor_token_type");
	if (actor_token_type_val.IsValid()) {
		TokenType actor_token_type_tmp;
		error = actor_token_type_tmp.TryFromJSON(actor_token_type_val);
		if (!error.empty()) {
			return error;
		}
		actor_token_type = std::move(actor_token_type_tmp);
	}
	return "";
}

void OAuthTokenExchangeRequest::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: grant_type
	auto grant_type_json = writer.CreateString(grant_type);
	obj.Add("grant_type", grant_type_json);

	// Serialize: subject_token
	auto subject_token_json = writer.CreateString(subject_token);
	obj.Add("subject_token", subject_token_json);

	// Serialize: subject_token_type
	auto subject_token_type_json = subject_token_type.ToJSON(writer);
	obj.Add("subject_token_type", subject_token_type_json);

	// Serialize: scope
	if (scope.has_value()) {
		auto &scope_value = *scope;
		auto scope_json = writer.CreateString(scope_value);
		obj.Add("scope", scope_json);
	}

	// Serialize: requested_token_type
	if (requested_token_type.has_value()) {
		auto &requested_token_type_value = *requested_token_type;
		auto requested_token_type_json = requested_token_type_value.ToJSON(writer);
		obj.Add("requested_token_type", requested_token_type_json);
	}

	// Serialize: actor_token
	if (actor_token.has_value()) {
		auto &actor_token_value = *actor_token;
		auto actor_token_json = writer.CreateString(actor_token_value);
		obj.Add("actor_token", actor_token_json);
	}

	// Serialize: actor_token_type
	if (actor_token_type.has_value()) {
		auto &actor_token_type_value = *actor_token_type;
		auto actor_token_type_json = actor_token_type_value.ToJSON(writer);
		obj.Add("actor_token_type", actor_token_type_json);
	}
}

JSONMutableValue OAuthTokenExchangeRequest::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
