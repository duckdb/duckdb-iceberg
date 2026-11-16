
#include "rest_catalog/objects/oauth_token_request.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

OAuthTokenRequest::OAuthTokenRequest() {
}

OAuthTokenRequest OAuthTokenRequest::FromJSON(JSONValue obj) {
	OAuthTokenRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

OAuthTokenRequest OAuthTokenRequest::Copy() const {
	OAuthTokenRequest res;
	if (oauth_client_credentials_request.has_value()) {
		res.oauth_client_credentials_request.emplace();
		(*res.oauth_client_credentials_request) = (*oauth_client_credentials_request).Copy();
	}
	if (oauth_token_exchange_request.has_value()) {
		res.oauth_token_exchange_request.emplace();
		(*res.oauth_token_exchange_request) = (*oauth_token_exchange_request).Copy();
	}
	return res;
}

string OAuthTokenRequest::TryFromJSON(JSONValue obj) {
	string error;
	oauth_client_credentials_request.emplace();
	error = oauth_client_credentials_request->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		oauth_client_credentials_request = nullopt;
	}
	oauth_token_exchange_request.emplace();
	error = oauth_token_exchange_request->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		oauth_token_exchange_request = nullopt;
	}
	if (!(oauth_client_credentials_request.has_value()) && !(oauth_token_exchange_request.has_value())) {
		return "OAuthTokenRequest failed to parse, none of the anyOf candidates matched";
	}
	return "";
}

void OAuthTokenRequest::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	if (oauth_client_credentials_request.has_value()) {
		oauth_client_credentials_request->PopulateJSON(writer, obj);
	} else if (oauth_token_exchange_request.has_value()) {
		oauth_token_exchange_request->PopulateJSON(writer, obj);
	}
}

JSONMutableValue OAuthTokenRequest::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
