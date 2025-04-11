
#include "rest_catalog/objects/oauth_token_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

OAuthTokenRequest::OAuthTokenRequest() {
}

OAuthTokenRequest OAuthTokenRequest::FromJSON(yyjson_val *obj) {
	OAuthTokenRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string OAuthTokenRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	error = oauth_client_credentials_request.TryFromJSON(obj);
	if (error.empty()) {
		has_oauth_client_credentials_request = true;
	}
	error = oauth_token_exchange_request.TryFromJSON(obj);
	if (error.empty()) {
		has_oauth_token_exchange_request = true;
	}
	if (!has_oauth_client_credentials_request && !has_oauth_token_exchange_request) {
		return "OAuthTokenRequest failed to parse, none of the anyOf candidates matched";
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
