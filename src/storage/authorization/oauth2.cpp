#include "iceberg_extension.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_logging.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "storage/authorization/oauth2.hpp"
#include "storage/catalog/iceberg_catalog.hpp"
#include "api_utils.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/value.hpp"
#include "rest_catalog/objects/oauth_token_response.hpp"
#include "rest_catalog/objects/oauth_error.hpp"
#include "duckdb/main/config.hpp"
#include <chrono>

namespace duckdb {

namespace {

//! NOTE: We sadly don't receive the CreateSecretFunction or some other context to deduplicate the recognized options
//! So we use this to deduplicate it instead
static const case_insensitive_map_t<LogicalType> &IcebergSecretOptions() {
	static const case_insensitive_map_t<LogicalType> options {
	    {"client_id", LogicalType::VARCHAR},
	    {"client_secret", LogicalType::VARCHAR},
	    {"endpoint", LogicalType::VARCHAR},
	    {"token", LogicalType::VARCHAR},
	    {"refresh_token", LogicalType::VARCHAR},
	    {"expires_in", LogicalType::INTEGER},
	    {"oauth2_scope", LogicalType::VARCHAR},
	    {"oauth2_server_uri", LogicalType::VARCHAR},
	    {"oauth2_grant_type", LogicalType::VARCHAR},
	    {"extra_http_headers", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)}};
	return options;
}

} // namespace

OAuth2Authorization::OAuth2Authorization() : IcebergAuthorization(IcebergAuthorizationType::OAUTH2) {
}

OAuth2Authorization::OAuth2Authorization(const string &grant_type, const string &uri, const string &client_id,
                                         const string &client_secret, const string &scope)
    : IcebergAuthorization(IcebergAuthorizationType::OAUTH2), grant_type(grant_type), uri(uri), client_id(client_id),
      client_secret(client_secret), scope(scope) {
}

//! NOTE: this doesnt use StringUtil::URLEncode(..., escape_slash=true) because of how ' ' (space) is encoded
namespace {

static string XWWWFormUrlEncode(const string &input) {
	string result;
	static const char *HEX_DIGIT = "0123456789ABCDEF";
	for (idx_t i = 0; i < input.size(); i++) {
		char ch = input[i];
		if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' ||
		    ch == '-' || ch == '~' || ch == '.') {
			result += ch;
		} else {
			result += '%';
			result += HEX_DIGIT[static_cast<unsigned char>(ch) >> 4];
			result += HEX_DIGIT[static_cast<unsigned char>(ch) & 15];
		}
	}
	return result;
}

static void ExtractOAuth2CredentialsFromSecret(const KeyValueSecret &kv_secret, OAuth2Authorization &result) {
	auto client_id_val = kv_secret.TryGetValue("client_id");
	if (!client_id_val.IsNull()) {
		result.client_id = client_id_val.ToString();
	}

	auto client_secret_val = kv_secret.TryGetValue("client_secret");
	if (!client_secret_val.IsNull()) {
		result.client_secret = client_secret_val.ToString();
	}

	auto oauth2_server_uri_val = kv_secret.TryGetValue("oauth2_server_uri");
	if (!oauth2_server_uri_val.IsNull()) {
		result.uri = oauth2_server_uri_val.ToString();
	}

	auto oauth2_grant_type_val = kv_secret.TryGetValue("oauth2_grant_type");
	if (!oauth2_grant_type_val.IsNull()) {
		result.grant_type = oauth2_grant_type_val.ToString();
	}

	auto oauth2_scope_val = kv_secret.TryGetValue("oauth2_scope");
	if (!oauth2_scope_val.IsNull()) {
		result.scope = oauth2_scope_val.ToString();
	}
}

static void ExtractOAuth2CredentialsFromOptions(const case_insensitive_map_t<Value> &options,
                                                OAuth2Authorization &result) {
	auto client_id_it = options.find("client_id");
	if (client_id_it != options.end()) {
		result.client_id = client_id_it->second.ToString();
	}

	auto client_secret_it = options.find("client_secret");
	if (client_secret_it != options.end()) {
		result.client_secret = client_secret_it->second.ToString();
	}

	auto oauth2_server_uri_it = options.find("oauth2_server_uri");
	if (oauth2_server_uri_it != options.end()) {
		result.uri = oauth2_server_uri_it->second.ToString();
	}

	auto oauth2_grant_type_it = options.find("oauth2_grant_type");
	if (oauth2_grant_type_it != options.end()) {
		result.grant_type = oauth2_grant_type_it->second.ToString();
	}

	auto oauth2_scope_it = options.find("oauth2_scope");
	if (oauth2_scope_it != options.end()) {
		result.scope = oauth2_scope_it->second.ToString();
	}
}

//! Helper function to fetch OAuth2 token and parse full response (RFC 6749).
//! Relies on DuckDB's built-in HTTP retry infrastructure (RunRequestWithRetry) for transient errors.
static rest_api_objects::OAuthTokenResponse FetchOAuth2TokenResponse(ClientContext &context, const string &grant_type,
                                                                     const string &uri, const string &client_id,
                                                                     const string &client_secret, const string &scope,
                                                                     const string &refresh_token_param = "") {
	vector<string> parameters;
	parameters.push_back(StringUtil::Format("%s=%s", XWWWFormUrlEncode("grant_type"), XWWWFormUrlEncode(grant_type)));

	// Google requires client credentials in POST body for refresh_token grant (not Basic Auth)
	// RFC 6749 Section 2.3.1 allows either method; we use POST body for refresh_token (Google),
	// Basic Auth for client_credentials (Keycloak/Polaris standard)
	bool use_body_auth = (grant_type == "refresh_token");

	if (grant_type == "refresh_token") {
		// RFC 6749 Section 6: Refreshing an Access Token
		// Google requires client credentials in POST body (not Basic Auth) for this grant
		parameters.push_back(
		    StringUtil::Format("%s=%s", XWWWFormUrlEncode("refresh_token"), XWWWFormUrlEncode(refresh_token_param)));
		parameters.push_back(StringUtil::Format("%s=%s", XWWWFormUrlEncode("client_id"), XWWWFormUrlEncode(client_id)));
		parameters.push_back(
		    StringUtil::Format("%s=%s", XWWWFormUrlEncode("client_secret"), XWWWFormUrlEncode(client_secret)));
		// Scope is optional for refresh_token grant. Only include if non-empty.
		// Omitting scope uses the original grant's scopes (Google requires this for user OAuth refresh_tokens).
		if (!scope.empty()) {
			parameters.push_back(StringUtil::Format("%s=%s", XWWWFormUrlEncode("scope"), XWWWFormUrlEncode(scope)));
		}
	} else {
		// client_credentials or other grant types - scope is required (always provided by caller)
		parameters.push_back(StringUtil::Format("%s=%s", XWWWFormUrlEncode("scope"), XWWWFormUrlEncode(scope)));
	}

	HTTPHeaders headers(*context.db);
	headers.Insert("Content-Type", "application/x-www-form-urlencoded");

	// Use Basic Auth for client_credentials, POST body credentials for refresh_token
	if (!use_body_auth) {
		string credentials = StringUtil::Format("%s:%s", client_id, client_secret);
		string_t credentials_blob(credentials.data(), credentials.size());
		headers.Insert("Authorization", StringUtil::Format("Basic %s", Blob::ToBase64(credentials_blob)));
	}

	string post_data = StringUtil::Join(parameters, "&");

	unique_ptr<HTTPResponse> response;
	try {
		auto endpoint_builder = IRCEndpointBuilder::FromURL(uri);
		unique_ptr<HTTPClient> placeholder_client;
		response = APIUtils::Request(RequestType::POST_REQUEST, context, endpoint_builder, placeholder_client, headers,
		                             post_data);
	} catch (std::exception &ex) {
		// Only catch actual transport/network errors (not HTTP errors)
		ErrorData error(ex);
		throw InvalidConfigurationException("Could not get token from %s: %s", uri, error.RawMessage());
	}

	// Check HTTP status code
	if (response->status >= HTTPStatusCode::OK_200 && response->status < HTTPStatusCode::MultipleChoices_300) {
		// Success: Parse OAuthTokenResponse
		auto *doc = yyjson_read(response->body.c_str(), response->body.size(), 0);
		if (!doc) {
			throw InvalidConfigurationException("Could not get token from %s: server returned invalid JSON", uri);
		}
		std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc_ptr(doc);
		auto *root = yyjson_doc_get_root(doc);
		auto token_response = rest_api_objects::OAuthTokenResponse::FromJSON(root);

		// Validate token_type is bearer
		if (!StringUtil::CIEquals(token_response.token_type, "bearer")) {
			throw NotImplementedException(
			    "token_type return value '%s' is not supported, only supports 'bearer' currently.",
			    token_response.token_type);
		}

		return token_response;
	} else if (response->status >= HTTPStatusCode::BadRequest_400 &&
	           response->status < HTTPStatusCode::InternalServerError_500) {
		// Client error: Try to parse OAuth2 error response (RFC 6749 Section 5.2)
		auto *doc = yyjson_read(response->body.c_str(), response->body.size(), 0);
		if (doc) {
			std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc_ptr(doc);
			auto *root = yyjson_doc_get_root(doc);
			try {
				auto oauth_error = rest_api_objects::OAuthError::FromJSON(root);
				string error_msg = StringUtil::Format("OAuth2 token request failed (%s): %s",
				                                      EnumUtil::ToString(response->status), oauth_error._error);
				if (oauth_error.has_error_description) {
					error_msg += StringUtil::Format(" - %s", oauth_error.error_description);
				}
				if (oauth_error.has_error_uri) {
					error_msg += StringUtil::Format(" (see %s)", oauth_error.error_uri);
				}
				throw InvalidConfigurationException(error_msg);
			} catch (InvalidInputException &) {
				// Not a valid OAuth error response, fall through to generic error
			}
		}
		// Generic client error
		throw InvalidConfigurationException("Could not get token from %s: HTTP %s - %s", uri,
		                                    EnumUtil::ToString(response->status), response->body);
	} else {
		// Server error or other status
		throw InvalidConfigurationException("Could not get token from %s: HTTP %s - %s", uri,
		                                    EnumUtil::ToString(response->status), response->reason);
	}
}

} // namespace

string OAuth2Authorization::GetToken(ClientContext &context, const string &grant_type, const string &uri,
                                     const string &client_id, const string &client_secret, const string &scope) {
	// Wrapper for backward compatibility - just returns the access_token
	auto token_response = FetchOAuth2TokenResponse(context, grant_type, uri, client_id, client_secret, scope);
	return token_response.access_token;
}

unique_ptr<OAuth2Authorization> OAuth2Authorization::FromAttachOptions(ClientContext &context,
                                                                       IcebergAttachOptions &input) {
	auto result = make_uniq<OAuth2Authorization>();

	unordered_map<string, Value> remaining_options;
	case_insensitive_map_t<Value> create_secret_options;
	string secret;
	Value token;

	static const unordered_set<string> recognized_create_secret_options {
	    "oauth2_scope", "oauth2_server_uri", "oauth2_grant_type",      "token",
	    "client_id",    "client_secret",     "access_delegation_mode", "extra_http_headers"};

	for (auto &entry : input.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "secret") {
			secret = entry.second.ToString();
		} else if (lower_name == "default_region") {
			result->default_region = entry.second.ToString();
		} else if (recognized_create_secret_options.count(lower_name)) {
			create_secret_options.emplace(std::move(entry));
		} else {
			remaining_options.emplace(std::move(entry));
		}
	}

	unique_ptr<SecretEntry> iceberg_secret;

	if (create_secret_options.empty()) {
		//! Look up an ICEBERG secret
		iceberg_secret = IcebergCatalog::GetIcebergSecret(context, secret);
		if (!iceberg_secret) {
			if (!secret.empty()) {
				throw InvalidConfigurationException("No ICEBERG secret by the name of '%s' could be found", secret);
			} else {
				throw InvalidConfigurationException(
				    "AUTHORIZATION_TYPE is 'oauth2', yet no 'secret' was provided, and no client_id+client_secret were "
				    "provided. Please provide one of the listed options or change the 'authorization_type'.");
			}
		}
		auto &kv_iceberg_secret = dynamic_cast<const KeyValueSecret &>(*iceberg_secret->secret);
		auto endpoint_from_secret = kv_iceberg_secret.TryGetValue("endpoint");
		if (input.endpoint.empty()) {
			if (endpoint_from_secret.IsNull()) {
				throw InvalidConfigurationException(
				    "No 'endpoint' was given to attach, and no 'endpoint' could be retrieved from the ICEBERG secret!");
			}
			DUCKDB_LOG(context, IcebergLogType, "'endpoint' is inferred from the ICEBERG secret '%s'",
			           iceberg_secret->secret->GetName());
			input.endpoint = endpoint_from_secret.ToString();
		}
		token = kv_iceberg_secret.TryGetValue("token");

		// Parse extra_http_headers from secret if present
		IcebergAuthorization::ParseExtraHttpHeaders(kv_iceberg_secret.TryGetValue("extra_http_headers"),
		                                            result->extra_http_headers);

		// Extract credentials for token refresh from existing secret
		ExtractOAuth2CredentialsFromSecret(kv_iceberg_secret, *result);

		// Extract refresh_token if present
		auto refresh_token_val = kv_iceberg_secret.TryGetValue("refresh_token");
		if (!refresh_token_val.IsNull()) {
			result->refresh_token = refresh_token_val.ToString();
		}

		// Extract expires_in for expiry calculation
		Value expires_in_val = kv_iceberg_secret.TryGetValue("expires_in");
		int32_t expires_in_seconds = 0;
		if (!expires_in_val.IsNull() && expires_in_val.type().id() == LogicalTypeId::INTEGER) {
			expires_in_seconds = expires_in_val.GetValue<int32_t>();
		}

		// Compute expiry time if we have both token and expires_in
		if (!token.IsNull() && expires_in_seconds > 0) {
			// Pass empty string for refresh_token to preserve the one we just set
			result->UpdateTokenState(token.ToString(), expires_in_seconds, "");
		}
	} else {
		if (!secret.empty()) {
			set<string> option_names;
			for (auto &entry : create_secret_options) {
				option_names.insert(entry.first);
			}
			throw InvalidConfigurationException(
			    "Both 'secret' and the following oauth2 option(s) were given: %s. These are mutually exclusive",
			    StringUtil::Join(option_names, ", "));
		}

		// Extract credentials from options BEFORE creating the secret
		// These will be needed for token refresh
		ExtractOAuth2CredentialsFromOptions(create_secret_options, *result);

		CreateSecretInput create_secret_input;
		if (!input.endpoint.empty()) {
			create_secret_options["endpoint"] = input.endpoint;
		}
		create_secret_input.options = std::move(create_secret_options);
		auto new_secret = OAuth2Authorization::CreateCatalogSecretFunction(context, create_secret_input);
		auto &kv_iceberg_secret = dynamic_cast<KeyValueSecret &>(*new_secret);
		token = kv_iceberg_secret.TryGetValue("token");

		// Extract refresh_token and expires_in from the newly created secret
		auto refresh_token_val = kv_iceberg_secret.TryGetValue("refresh_token");
		if (!refresh_token_val.IsNull()) {
			result->refresh_token = refresh_token_val.ToString();
		}

		auto expires_in_val = kv_iceberg_secret.TryGetValue("expires_in");
		if (!expires_in_val.IsNull() && expires_in_val.type().id() == LogicalTypeId::INTEGER) {
			int32_t expires_in = expires_in_val.GetValue<int32_t>();
			// Compute expiry time when we have the token
			if (!token.IsNull()) {
				// Pass empty string for refresh_token to preserve the one we just set
				result->UpdateTokenState(token.ToString(), expires_in, "");
			}
		}

		// Parse extra_http_headers from inline options if present
		IcebergAuthorization::ParseExtraHttpHeaders(kv_iceberg_secret.TryGetValue("extra_http_headers"),
		                                            result->extra_http_headers);
	}

	if (token.IsNull()) {
		throw HTTPException(StringUtil::Format("Failed to retrieve OAuth2 token from %s", result->uri));
	}
	result->token = token.ToString();

	input.options = std::move(remaining_options);
	return result;
}

unique_ptr<BaseSecret> OAuth2Authorization::CreateCatalogSecretFunction(ClientContext &context,
                                                                        CreateSecretInput &input) {
	// apply any overridden settings
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, "iceberg", "config", input.name);
	result->redact_keys = {"token", "client_id", "client_secret", "refresh_token"};

	auto &accepted_parameters = IcebergSecretOptions();

	for (const auto &named_param : input.options) {
		auto &param_name = named_param.first;
		auto it = accepted_parameters.find(param_name);
		if (it != accepted_parameters.end()) {
			// Special handling for extra_http_headers (MAP type)
			if (StringUtil::Lower(param_name) == "extra_http_headers") {
				// Store the MAP value directly, will be parsed later when creating authorization
				result->secret_map[param_name] = named_param.second;
			} else if (StringUtil::Lower(param_name) == "expires_in") {
				// Store expires_in as INTEGER (not string)
				result->secret_map[param_name] = named_param.second;
			} else {
				result->secret_map[param_name] = named_param.second.ToString();
			}
		} else {
			throw InvalidInputException("Unknown named parameter passed to CreateIRCSecretFunction: %s", param_name);
		}
	}

	//! ---- Token ----
	auto token_it = result->secret_map.find("token");
	if (token_it != result->secret_map.end()) {
		return std::move(result);
	}

	//! ---- Server URI (and Endpoint) ----
	string server_uri;
	auto oauth2_server_uri_it = result->secret_map.find("oauth2_server_uri");
	auto endpoint_it = result->secret_map.find("endpoint");
	if (oauth2_server_uri_it != result->secret_map.end()) {
		server_uri = oauth2_server_uri_it->second.ToString();
	} else if (endpoint_it != result->secret_map.end()) {
		DUCKDB_LOG(
		    context, IcebergLogType,
		    "'oauth2_server_uri' is not set, defaulting to deprecated '{endpoint}/v1/oauth/tokens' oauth2_server_uri");
		server_uri = StringUtil::Format("%s/v1/oauth/tokens", endpoint_it->second.ToString());
	} else {
		throw InvalidConfigurationException(
		    "AUTHORIZATION_TYPE is 'oauth2', yet no 'oauth2_server_uri' was provided, and no 'endpoint' was provided "
		    "to fall back on. Please provide one or change the 'authorization_type'.");
	}

	//! ---- Client ID + Client Secret ----
	case_insensitive_set_t required_parameters {"client_id", "client_secret"};
	for (auto &param : required_parameters) {
		if (!result->secret_map.count(param)) {
			throw InvalidInputException("Missing required parameter '%s' for authorization_type 'oauth2'", param);
		}
	}

	//! ---- Grant Type and Token Acquisition ----
	// Determine which grant type to use for initial token acquisition
	string grant_type_to_use;
	string refresh_token_param;
	string scope_to_use;

	// Check if refresh_token was provided
	auto refresh_token_it = result->secret_map.find("refresh_token");
	if (refresh_token_it != result->secret_map.end()) {
		// User provided a refresh_token - use refresh_token grant
		grant_type_to_use = "refresh_token";
		refresh_token_param = refresh_token_it->second.ToString();
		// Don't send scope in refresh_token grant (use original token scopes)
		// Per RFC 6749 Section 6, scope is optional and if omitted, is treated as equal to original scope
		auto scope_it = result->secret_map.find("oauth2_scope");
		scope_to_use = (scope_it != result->secret_map.end()) ? scope_it->second.ToString() : "";
	} else {
		// No refresh_token - use client_credentials grant
		auto grant_type_it = result->secret_map.find("oauth2_grant_type");
		if (grant_type_it != result->secret_map.end()) {
			grant_type_to_use = grant_type_it->second.ToString();
			if (!StringUtil::CIEquals(grant_type_to_use, "client_credentials")) {
				throw InvalidInputException(
				    "Unsupported option ('%s') for 'oauth2_grant_type', only supports 'client_credentials' currently",
				    grant_type_to_use);
			}
		} else {
			grant_type_to_use = "client_credentials";
		}
		// Default scope for client_credentials grant
		if (!result->secret_map.count("oauth2_scope")) {
			result->secret_map["oauth2_scope"] = "PRINCIPAL_ROLE:ALL";
		}
		scope_to_use = result->secret_map["oauth2_scope"].ToString();
	}

	// Make a request to the oauth2 server uri to get the (bearer) token
	// Store the full response to capture expires_in and refresh_token
	auto token_response =
	    FetchOAuth2TokenResponse(context, grant_type_to_use, server_uri, result->secret_map["client_id"].ToString(),
	                             result->secret_map["client_secret"].ToString(), scope_to_use, refresh_token_param);

	result->secret_map["token"] = token_response.access_token;

	// Store refresh_token if present (RFC 6749 Section 6)
	if (token_response.has_refresh_token && !token_response.refresh_token.empty()) {
		result->secret_map["refresh_token"] = token_response.refresh_token;
	}

	// Store expires_in if present
	if (token_response.has_expires_in) {
		result->secret_map["expires_in"] = Value::INTEGER(token_response.expires_in);
	}

	// Store the credentials for later refresh (already in secret_map)
	// We keep client_id, client_secret, oauth2_server_uri, oauth2_scope for refresh

	return std::move(result);
}

unique_ptr<HTTPResponse> OAuth2Authorization::Request(RequestType request_type, ClientContext &context,
                                                      const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
                                                      const string &data) {
	// --- Step 1: Proactive refresh under lock, then copy token ---
	// Serialized refresh: at most one thread refreshes at a time.
	// Refresh I/O under lock is acceptable (rare, bounded by token lifetime).
	// Threads that queue behind the mutex will re-check expiry, see the
	// fresh token, and skip refresh.
	string bearer_token;
	{
		std::lock_guard<std::mutex> lock(token_mutex);
		if (IsTokenExpiredUnlocked(context, lock) && CanRefreshUnlocked(lock)) {
			RefreshAccessTokenUnlocked(context, lock);
		}
		bearer_token = token;
	}
	// Lock released -- catalog HTTP request runs concurrently with other threads.

	// --- Step 2: Build headers and make the catalog request ---
	for (auto &entry : extra_http_headers) {
		headers.Insert(entry.first, entry.second);
	}
	if (!bearer_token.empty()) {
		headers["Authorization"] = StringUtil::Format("Bearer %s", bearer_token);
	}

	auto response = APIUtils::Request(request_type, context, endpoint_builder, client, headers, data);

	// --- Step 3: Reactive 401 refresh (exactly once) ---
	// If the server rejected our token (e.g., revoked before expiry, clock skew,
	// audience change), refresh once and retry. Guard against infinite loops.
	if (response->status == HTTPStatusCode::Unauthorized_401) {
		bool should_retry = false;
		{
			std::lock_guard<std::mutex> lock(token_mutex);
			if (CanRefreshUnlocked(lock)) {
				RefreshAccessTokenUnlocked(context, lock);
				bearer_token = token;
				should_retry = true;
			}
		}
		// Lock released before retry -- avoid serializing catalog requests
		if (should_retry) {
			headers["Authorization"] = StringUtil::Format("Bearer %s", bearer_token);
			response = APIUtils::Request(request_type, context, endpoint_builder, client, headers, data);
		}
	}

	return response;
}

void OAuth2Authorization::SetCatalogSecretParameters(CreateSecretFunction &function) {
	auto &options = IcebergSecretOptions();
	function.named_parameters.insert(options.begin(), options.end());
}

void OAuth2Authorization::UpdateTokenState(const string &new_token, int32_t expires_in_seconds,
                                           const string &new_refresh_token) {
	token = new_token;

	// Only update refresh_token if a new one is provided (RFC 6749 Section 6)
	// "The authorization server MAY issue a new refresh token, in which case
	// the client MUST discard the old refresh token and replace it with the new one."
	// If no new refresh_token is provided, keep the existing one.
	//
	// NOTE: Rotation is in-memory only. The original secret in SecretManager is NOT updated.
	// After DETACH + re-ATTACH, the rotated refresh_token is lost and the client falls back
	// to client_credentials if available. This is a known limitation.
	if (!new_refresh_token.empty()) {
		refresh_token = new_refresh_token;
	}

	// Determine which expires_in to use
	int32_t effective_expires_in = expires_in_seconds;
	if (expires_in_seconds <= 0) {
		// Server omitted expires_in: reuse previous value if available, else use conservative default
		if (last_expires_in > 0) {
			effective_expires_in = last_expires_in;
		} else {
			// No previous expiry known: apply conservative default (1 hour)
			effective_expires_in = 3600;
		}
	} else {
		// Store the new expires_in for future reuse
		last_expires_in = expires_in_seconds;
	}

	if (effective_expires_in > 0) {
		// Calculate expiry time with safety buffer (clamped to avoid negative durations)
		auto now = std::chrono::system_clock::now();
		auto buffer_seconds =
		    std::min(30, effective_expires_in / 2); // Use 30s or half the lifetime, whichever is smaller
		auto expiry_duration = std::chrono::seconds(effective_expires_in - buffer_seconds);
		auto expiry_time = now + expiry_duration;
		token_expires_at = std::chrono::duration_cast<std::chrono::seconds>(expiry_time.time_since_epoch()).count();
	} else {
		// No expiry information available at all (shouldn't happen with the logic above)
		token_expires_at = 0;
	}
}

bool OAuth2Authorization::IsTokenExpiredUnlocked(ClientContext &context, std::lock_guard<std::mutex> &lock) const {
	// Internal method - caller must hold token_mutex
	(void)lock;

	// Test hook to force token expiry (for test infrastructure)
	Value force_expiry_val;
	if (context.TryGetCurrentSetting("iceberg_test_force_token_expiry", force_expiry_val)) {
		if (!force_expiry_val.IsNull() && force_expiry_val.type().id() == LogicalTypeId::BOOLEAN &&
		    force_expiry_val.GetValue<bool>()) {
			return true;
		}
	}

	// Check normal expiry
	if (token_expires_at == 0) {
		// No expiry set = token never expires (static token or no expires_in in response)
		return false;
	}

	auto now = std::chrono::system_clock::now();
	auto now_seconds = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
	return now_seconds >= token_expires_at;
}

bool OAuth2Authorization::CanRefreshUnlocked(std::lock_guard<std::mutex> &lock) const {
	// Internal method - caller must hold token_mutex
	(void)lock;
	// Can refresh if we have a refresh_token or if we have client credentials
	return !refresh_token.empty() || (!client_id.empty() && !client_secret.empty() && !uri.empty());
}

void OAuth2Authorization::RefreshAccessTokenUnlocked(ClientContext &context, std::lock_guard<std::mutex> &lock) {
	// Internal method - caller must hold token_mutex
	(void)lock;
	if (!CanRefreshUnlocked(lock)) {
		throw HTTPException("Cannot refresh access token: no refresh_token and no client credentials available");
	}

	rest_api_objects::OAuthTokenResponse token_response;

	if (!refresh_token.empty()) {
		// RFC 6749 Section 6: Use refresh_token grant
		// Try refresh_token first, fall back to client_credentials if it fails
		try {
			token_response =
			    FetchOAuth2TokenResponse(context, "refresh_token", uri, client_id, client_secret, scope, refresh_token);
		} catch (std::exception &ex) {
			// Refresh token grant failed (e.g., token revoked, invalid_grant error)
			// Fall back to client_credentials if available
			if (!client_id.empty() && !client_secret.empty() && !uri.empty()) {
				// Clear the stale refresh_token to avoid repeated failures
				refresh_token.clear();
				string effective_grant_type = grant_type.empty() ? "client_credentials" : grant_type;
				token_response =
				    FetchOAuth2TokenResponse(context, effective_grant_type, uri, client_id, client_secret, scope);
			} else {
				// No fallback available, re-throw the original error
				throw;
			}
		}
	} else {
		// No refresh_token: Re-acquire token using client_credentials grant
		string effective_grant_type = grant_type.empty() ? "client_credentials" : grant_type;
		token_response = FetchOAuth2TokenResponse(context, effective_grant_type, uri, client_id, client_secret, scope);
	}

	// Update our token state with the new token (UpdateTokenState assumes lock is held)
	UpdateTokenState(token_response.access_token, token_response.has_expires_in ? token_response.expires_in : 0,
	                 token_response.has_refresh_token ? token_response.refresh_token : "");
}

} // namespace duckdb
