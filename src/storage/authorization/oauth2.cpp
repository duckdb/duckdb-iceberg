#include "iceberg_extension.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_logging.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "storage/authorization/oauth2.hpp"
#include "storage/irc_catalog.hpp"
#include "api_utils.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/types/blob.hpp"

namespace duckdb {

namespace {

//! NOTE: We sadly don't receive the CreateSecretFunction or some other context to deduplicate the recognized options
//! So we use this to deduplicate it instead
static const case_insensitive_map_t<LogicalType> &IcebergSecretOptions() {
	static const case_insensitive_map_t<LogicalType> options {
	    {"client_id", LogicalType::VARCHAR},        {"client_secret", LogicalType::VARCHAR},
	    {"endpoint", LogicalType::VARCHAR},         {"token", LogicalType::VARCHAR},
	    {"oauth2_scope", LogicalType::VARCHAR},     {"oauth2_server_uri", LogicalType::VARCHAR},
	    {"oauth2_grant_type", LogicalType::VARCHAR}};
	return options;
}

} // namespace

OAuth2Authorization::OAuth2Authorization() : IRCAuthorization(IRCAuthorizationType::OAUTH2) {
}

OAuth2Authorization::OAuth2Authorization(const string &grant_type, const string &uri, const string &client_id,
                                         const string &client_secret, const string &scope)
    : IRCAuthorization(IRCAuthorizationType::OAUTH2), grant_type(grant_type), uri(uri), client_id(client_id),
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

} // namespace

string OAuth2Authorization::GetToken(ClientContext &context, const string &grant_type, const string &uri,
                                     const string &client_id, const string &client_secret, const string &scope) {
	vector<string> parameters;
	parameters.push_back(StringUtil::Format("%s=%s", XWWWFormUrlEncode("grant_type"), XWWWFormUrlEncode(grant_type)));
	parameters.push_back(StringUtil::Format("%s=%s", XWWWFormUrlEncode("scope"), XWWWFormUrlEncode(scope)));

	string credentials = StringUtil::Format("%s:%s", client_id, client_secret);
	string_t credentials_blob(credentials.data(), credentials.size());

	HTTPHeaders headers(*context.db);
	headers.Insert("Authorization", StringUtil::Format("Basic %s", Blob::ToBase64(credentials_blob)));
	headers.Insert("Content-Type", StringUtil::Format("application/%s", "x-www-form-urlencoded"));
	string post_data = StringUtil::Format("%s", StringUtil::Join(parameters, "&"));
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc;
	try {
		auto endpoint_builder = IRCEndpointBuilder::FromURL(uri);
		unique_ptr<HTTPClient> placeholder_client;
		auto response = APIUtils::Request(RequestType::POST_REQUEST, context, endpoint_builder, placeholder_client,
		                                  headers, post_data);
		doc = std::unique_ptr<yyjson_doc, YyjsonDocDeleter>(ICUtils::api_result_to_doc(response->body));
	} catch (std::exception &ex) {
		ErrorData error(ex);
		throw InvalidConfigurationException("Could not get token from %s, captured error message: %s", uri,
		                                    error.RawMessage());
	}
	//! FIXME: the oauth/tokens endpoint returns, on success;
	// { 'access_token', 'token_type', 'expires_in', <issued_token_type>, 'refresh_token', 'scope'}
	auto *root = yyjson_doc_get_root(doc.get());
	auto access_token_val = yyjson_obj_get(root, "access_token");
	auto token_type_val = yyjson_obj_get(root, "token_type");
	if (!access_token_val) {
		throw InvalidConfigurationException("OAuthTokenResponse is missing required property 'access_token'");
	}
	if (!token_type_val) {
		throw InvalidConfigurationException("OAuthTokenResponse is missing required property 'token_type'");
	}
	string token_type = yyjson_get_str(token_type_val);
	if (!StringUtil::CIEquals(token_type, "bearer")) {
		throw NotImplementedException(
		    "token_type return value '%s' is not supported, only supports 'bearer' currently.", token_type);
	}
	string access_token = yyjson_get_str(access_token_val);
	return access_token;
}

unique_ptr<OAuth2Authorization> OAuth2Authorization::FromAttachOptions(ClientContext &context,
                                                                       IcebergAttachOptions &input) {
	auto result = make_uniq<OAuth2Authorization>();

	unordered_map<string, Value> remaining_options;
	case_insensitive_map_t<Value> create_secret_options;
	string secret;
	Value token;

	static const unordered_set<string> recognized_create_secret_options {
	    "oauth2_scope", "oauth2_server_uri", "oauth2_grant_type",     "token",
	    "client_id",    "client_secret",     "access_delegation_mode"};

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
		iceberg_secret = IRCatalog::GetIcebergSecret(context, secret);
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
		CreateSecretInput create_secret_input;
		if (!input.endpoint.empty()) {
			create_secret_options["endpoint"] = input.endpoint;
		}
		create_secret_input.options = std::move(create_secret_options);
		auto new_secret = OAuth2Authorization::CreateCatalogSecretFunction(context, create_secret_input);
		auto &kv_iceberg_secret = dynamic_cast<KeyValueSecret &>(*new_secret);
		token = kv_iceberg_secret.TryGetValue("token");
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
	result->redact_keys = {"token", "client_id", "client_secret"};

	auto &accepted_parameters = IcebergSecretOptions();

	for (const auto &named_param : input.options) {
		auto &param_name = named_param.first;
		auto it = accepted_parameters.find(param_name);
		if (it != accepted_parameters.end()) {
			result->secret_map[param_name] = named_param.second.ToString();
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

	//! ---- Grant Type ----
	auto grant_type_it = result->secret_map.find("oauth2_grant_type");
	if (grant_type_it != result->secret_map.end()) {
		auto grant_type = grant_type_it->second.ToString();
		if (!StringUtil::CIEquals(grant_type, "client_credentials")) {
			throw InvalidInputException(
			    "Unsupported option ('%s') for 'oauth2_grant_type', only supports 'client_credentials' currently",
			    grant_type);
		}
	} else {
		//! Default to client_credentials
		result->secret_map["oauth2_grant_type"] = "client_credentials";
	}

	//! ---- Scope ----
	if (!result->secret_map.count("oauth2_scope")) {
		//! Default to default Polaris role
		result->secret_map["oauth2_scope"] = "PRINCIPAL_ROLE:ALL";
	}

	// Make a request to the oauth2 server uri to get the (bearer) token
	result->secret_map["token"] = OAuth2Authorization::GetToken(
	    context, result->secret_map["oauth2_grant_type"].ToString(), server_uri,
	    result->secret_map["client_id"].ToString(), result->secret_map["client_secret"].ToString(),
	    result->secret_map["oauth2_scope"].ToString());
	return std::move(result);
}

unique_ptr<HTTPResponse> OAuth2Authorization::Request(RequestType request_type, ClientContext &context,
                                                      const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
                                                      const string &data) {
	if (!token.empty()) {
		headers.Insert("Authorization", StringUtil::Format("Bearer %s", token));
	}
	return APIUtils::Request(request_type, context, endpoint_builder, client, headers, data);
}

void OAuth2Authorization::SetCatalogSecretParameters(CreateSecretFunction &function) {
	auto &options = IcebergSecretOptions();
	function.named_parameters.insert(options.begin(), options.end());
}

} // namespace duckdb
