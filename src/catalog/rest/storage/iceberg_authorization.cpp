
#include "catalog/rest/storage/iceberg_authorization.hpp"

#include "duckdb/common/types/value.hpp"

#include "catalog/rest/api/api_utils.hpp"
#include "catalog/rest/storage/authorization/oauth2.hpp"

namespace duckdb {

unique_ptr<HTTPClient> &IcebergAuthorizationContextState::GetHTTPClient(AttachedDatabase &db, ClientContext &context) {
	auto instance = context.registered_state->GetOrCreate<IcebergAuthorizationContextState>("iceberg_authorization");
	auto res = instance->client_map.emplace(reinterpret_cast<uintptr_t>(&db), nullptr);
	return res.first->second;
}

IcebergAuthorizationType IcebergAuthorization::TypeFromString(const string &type) {
	static const case_insensitive_map_t<IcebergAuthorizationType> mapping {{"oauth2", IcebergAuthorizationType::OAUTH2},
	                                                                       {"sigv4", IcebergAuthorizationType::SIGV4},
	                                                                       {"none", IcebergAuthorizationType::NONE}};

	for (auto it : mapping) {
		if (StringUtil::CIEquals(it.first, type)) {
			return it.second;
		}
	}

	set<string> accepted_options;
	for (auto it : mapping) {
		accepted_options.insert(it.first);
	}
	throw InvalidConfigurationException("'authorization_type' '%s' is not supported, valid options are: %s", type,
	                                    StringUtil::Join(accepted_options, ", "));
}

case_insensitive_map_t<Value> IcebergAuthorization::CreateConfigurationMapDefaults(ClientContext &context) const {
	return {};
}

static void ParseGCSConfigOptions(const case_insensitive_map_t<string> &config,
                                  case_insensitive_map_t<Value> &options) {
	// Parse GCS-specific configuration.
	auto token_it = config.find("gcs.oauth2.token");
	if (token_it != config.end()) {
		options["bearer_token"] = token_it->second;
	}
}

static void ParseAzureConfigOptions(const case_insensitive_map_t<string> &config,
                                    case_insensitive_map_t<Value> &options) {
	static const string ADLS_SAS_TOKEN_PREFIX = "adls.sas-token.";

	for (auto &entry : config) {
		// SAS token config format is e.g. {adls.sas-token.<account-name>.dfs.core.windows.net, <token>}
		if (!StringUtil::StartsWith(entry.first, ADLS_SAS_TOKEN_PREFIX)) {
			continue;
		}
		string host = entry.first.substr(ADLS_SAS_TOKEN_PREFIX.length());
		// Extract account name
		auto dot_pos = StringUtil::Find(host, ".");
		string account_name = dot_pos.IsValid() ? host.substr(0, dot_pos.GetIndex()) : host;

		if (!account_name.empty() && !entry.second.empty()) {
			options["account_name"] = account_name;
			options["connection_string"] =
			    StringUtil::Format("AccountName=%s;SharedAccessSignature=%s", account_name, entry.second);

			// For now, only process the first {storage account, token} pair we find in the config
			return;
		}
	}
}

static void ParseS3ConfigOptions(const case_insensitive_map_t<string> &config, case_insensitive_map_t<Value> &options) {
	// Set of recognized S3 config parameters and the duckdb secret option that matches it.
	static const case_insensitive_map_t<string> config_to_option = {{"s3.access-key-id", "key_id"},
	                                                                {"s3.secret-access-key", "secret"},
	                                                                {"s3.session-token", "session_token"},
	                                                                {"s3.region", "region"},
	                                                                {"region", "region"},
	                                                                {"client.region", "region"},
	                                                                {"s3.endpoint", "endpoint"}};

	for (auto &entry : config) {
		auto it = config_to_option.find(entry.first);
		if (it != config_to_option.end()) {
			options[it->second] = entry.second;
		}
	}
}

static string RetrieveRegion(DBConfig &db_config) {
	char *retrieved_region = std::getenv("AWS_REGION");
	if (retrieved_region) {
		return string(retrieved_region);
	}
	retrieved_region = std::getenv("AWS_DEFAULT_REGION");
	if (retrieved_region) {
		return string(retrieved_region);
	}

	Value region_value;
	if (db_config.TryGetCurrentSetting("s3_region", region_value)) {
		return region_value.ToString();
	}
	return "";
}

void IcebergAuthorization::ParseConfigOptions(const case_insensitive_map_t<string> &config, ClientContext &context,
                                              const string &storage_type, case_insensitive_map_t<Value> &options_out) {
	// Parse storage-specific config options
	if (storage_type == "gcs") {
		ParseGCSConfigOptions(config, options_out);
	} else if (storage_type == "azure") {
		ParseAzureConfigOptions(config, options_out);
	} else {
		// Default to S3 parsing for backward compatibility
		ParseS3ConfigOptions(config, options_out);

		if (options_out.find("region") == options_out.end()) {
			const string region = RetrieveRegion(DBConfig::GetConfig(context));

			if (region.empty()) {
				throw InvalidConfigurationException(
				    "No region was provided via the vended credentials, and no region could be found via "
				    "environment variables. Please provide a default_region for the Iceberg Catalog when attaching");
			}
			options_out["region"] = Value(region);
		}
	}

	auto it = config.find("s3.path-style-access");
	if (it != config.end()) {
		bool path_style;
		if (it->second == "true") {
			path_style = true;
		} else if (it->second == "false") {
			path_style = false;
		} else {
			throw InvalidInputException("Unexpected value ('%s') for 's3.path-style-access' in 'config' property",
			                            it->second);
		}

		options_out["use_ssl"] = Value(!path_style);
		if (path_style) {
			options_out["url_style"] = "path";
		}
	}

	auto endpoint_it = options_out.find("endpoint");
	if (endpoint_it == options_out.end()) {
		return;
	}

	//! Adjust the 'endpoint' if necessary
	auto endpoint = endpoint_it->second.ToString();
	if (StringUtil::StartsWith(endpoint, "http://")) {
		endpoint = endpoint.substr(7, string::npos);
	} else if (StringUtil::StartsWith(endpoint, "https://")) {
		endpoint = endpoint.substr(8, string::npos);
		// if there is an endpoint and the endpoiont has https, use ssl.
		options_out["use_ssl"] = Value(true);
	} else if (StringUtil::EndsWith(endpoint, "/")) {
		endpoint = endpoint.substr(0, endpoint.size() - 1);
	}
	endpoint_it->second = endpoint;
	return;
}

void IcebergAuthorization::ParseExtraHttpHeaders(const Value &headers_value,
                                                 unordered_map<string, string> &out_headers) {
	if (headers_value.IsNull() || headers_value.type().id() != LogicalTypeId::MAP) {
		return;
	}

	// MAP is internally a LIST<STRUCT(key, value)>
	// Each entry in the list is a STRUCT with exactly two fields: key and value
	auto &map_entries = MapValue::GetChildren(headers_value);

	for (const auto &entry : map_entries) {
		if (entry.type().id() != LogicalTypeId::STRUCT) {
			continue;
		}

		auto &struct_children = StructValue::GetChildren(entry);
		if (struct_children.size() != 2) {
			continue;
		}

		// struct_children[0] = key, struct_children[1] = value
		out_headers[struct_children[0].ToString()] = struct_children[1].ToString();
	}
}

} // namespace duckdb
