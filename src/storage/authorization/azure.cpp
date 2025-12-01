#include "storage/authorization/azure.hpp"
#include "storage/irc_catalog.hpp"
#include "api_utils.hpp"
#include "iceberg_logging.hpp"
#include "duckdb/common/exception/http_exception.hpp"

#if DUCKDB_ICEBERG_AZURE_SUPPORT
#include <azure/core/credentials/token_credential_options.hpp>
#include <azure/core/http/policies/policy.hpp>
#include <azure/identity/azure_cli_credential.hpp>
#include <azure/identity/chained_token_credential.hpp>
#endif

namespace duckdb {

namespace {

static const case_insensitive_map_t<LogicalType> &IcebergAzureSecretOptions() {
	static const case_insensitive_map_t<LogicalType> options {
	    {"account_name", LogicalType::VARCHAR},
	    {"chain", LogicalType::VARCHAR},
	    {"endpoint", LogicalType::VARCHAR}};
	return options;
}

#if DUCKDB_ICEBERG_AZURE_SUPPORT
static Azure::Core::Credentials::TokenCredentialOptions GetTokenCredentialOptions() {
	Azure::Core::Credentials::TokenCredentialOptions options;
	return options;
}

static std::shared_ptr<Azure::Core::Credentials::TokenCredential>
CreateChainedTokenCredential(const string &chain) {
	auto credential_options = GetTokenCredentialOptions();

	auto chain_list = StringUtil::Split(chain, ';');
	Azure::Identity::ChainedTokenCredential::Sources sources;
	for (const auto &item : chain_list) {
		if (item == "cli") {
			sources.push_back(std::make_shared<Azure::Identity::AzureCliCredential>(credential_options));
		} else {
			throw InvalidInputException("Unknown credential provider found: " + item);
		}
	}
	return std::make_shared<Azure::Identity::ChainedTokenCredential>(sources);
}

static string GetTokenFromCredential(std::shared_ptr<Azure::Core::Credentials::TokenCredential> credential) {
	Azure::Core::Credentials::TokenRequestContext token_request_context;
	token_request_context.Scopes = {"https://storage.azure.com/.default"};

	try {
		auto token = credential->GetToken(token_request_context, Azure::Core::Context());
		return token.Token;
	} catch (const std::exception &ex) {
		throw InvalidConfigurationException("Failed to retrieve Azure token: %s", ex.what());
	}
}
#endif

} // namespace

AzureAuthorization::AzureAuthorization() : IRCAuthorization(IRCAuthorizationType::AZURE) {
}

AzureAuthorization::AzureAuthorization(const string &account_name, const string &chain)
    : IRCAuthorization(IRCAuthorizationType::AZURE), account_name(account_name), chain(chain) {
}

unique_ptr<AzureAuthorization> AzureAuthorization::FromAttachOptions(ClientContext &context,
                                                                     IcebergAttachOptions &input) {
	auto result = make_uniq<AzureAuthorization>();

	unordered_map<string, Value> remaining_options;
	case_insensitive_map_t<Value> create_secret_options;
	string secret;

	static const unordered_set<string> recognized_create_secret_options {
	    "account_name", "chain", "endpoint"};

	for (auto &entry : input.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "secret") {
			secret = entry.second.ToString();
		} else if (recognized_create_secret_options.count(lower_name)) {
			create_secret_options.emplace(std::move(entry));
		} else {
			remaining_options.emplace(std::move(entry));
		}
	}

	unique_ptr<SecretEntry> iceberg_secret;
	if (create_secret_options.empty()) {
		iceberg_secret = IRCatalog::GetIcebergSecret(context, secret);
		if (!iceberg_secret) {
			if (!secret.empty()) {
				throw InvalidConfigurationException("No ICEBERG secret by the name of '%s' could be found", secret);
			} else {
				throw InvalidConfigurationException(
				    "AUTHORIZATION_TYPE is 'azure', yet no 'secret' was provided, and no account_name+chain were "
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

		auto account_name_val = kv_iceberg_secret.TryGetValue("account_name");
		if (!account_name_val.IsNull()) {
			result->account_name = account_name_val.ToString();
		}

		auto chain_val = kv_iceberg_secret.TryGetValue("chain");
		if (!chain_val.IsNull()) {
			result->chain = chain_val.ToString();
		}

		auto token_val = kv_iceberg_secret.TryGetValue("token");
		if (!token_val.IsNull()) {
			result->token = token_val.ToString();
		}
	} else {
		if (!secret.empty()) {
			set<string> option_names;
			for (auto &entry : create_secret_options) {
				option_names.insert(entry.first);
			}
			throw InvalidConfigurationException(
			    "Both 'secret' and the following Azure option(s) were given: %s. These are mutually exclusive",
			    StringUtil::Join(option_names, ", "));
		}
		CreateSecretInput create_secret_input;
		if (!input.endpoint.empty()) {
			create_secret_options["endpoint"] = input.endpoint;
		}
		create_secret_input.options = std::move(create_secret_options);
		auto new_secret = AzureAuthorization::CreateCatalogSecretFunction(context, create_secret_input);
		auto &kv_iceberg_secret = dynamic_cast<KeyValueSecret &>(*new_secret);

		auto account_name_val = kv_iceberg_secret.TryGetValue("account_name");
		if (!account_name_val.IsNull()) {
			result->account_name = account_name_val.ToString();
		}

		auto chain_val = kv_iceberg_secret.TryGetValue("chain");
		if (!chain_val.IsNull()) {
			result->chain = chain_val.ToString();
		}

		auto token_val = kv_iceberg_secret.TryGetValue("token");
		if (!token_val.IsNull()) {
			result->token = token_val.ToString();
		}
	}

	if (result->token.empty()) {
		throw HTTPException(StringUtil::Format("Failed to retrieve Azure token from credential chain '%s'",
		                                       result->chain));
	}	input.options = std::move(remaining_options);
	return result;
}

unique_ptr<BaseSecret> AzureAuthorization::CreateCatalogSecretFunction(ClientContext &context,
                                                                       CreateSecretInput &input) {
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, "iceberg", "credential_chain", input.name);
	result->redact_keys = {"token"};

	auto &accepted_parameters = IcebergAzureSecretOptions();

	for (const auto &named_param : input.options) {
		auto &param_name = named_param.first;
		auto it = accepted_parameters.find(param_name);
		if (it != accepted_parameters.end()) {
			result->secret_map[param_name] = named_param.second.ToString();
		} else {
			throw InvalidInputException("Unknown named parameter passed to CreateAzureIcebergSecretFunction: %s", param_name);
		}
	}

	auto token_it = result->secret_map.find("token");
	if (token_it != result->secret_map.end()) {
		return std::move(result);
	}

	auto account_name_it = result->secret_map.find("account_name");
	if (account_name_it == result->secret_map.end()) {
		throw InvalidConfigurationException(
		    "Missing required parameter 'account_name' for authorization_type 'azure' with credential_chain provider");
	}

	auto chain_it = result->secret_map.find("chain");
	string chain = "cli";
	if (chain_it != result->secret_map.end()) {
		chain = chain_it->second.ToString();
	} else {
		result->secret_map["chain"] = chain;
	}

	if (!StringUtil::CIEquals(chain, "cli")) {
		throw InvalidInputException(
		    "Unsupported option ('%s') for 'chain', only supports 'cli' currently", chain);
	}

#if DUCKDB_ICEBERG_AZURE_SUPPORT
	auto credential = CreateChainedTokenCredential(chain);
	result->secret_map["token"] = GetTokenFromCredential(credential);
#else
	throw NotImplementedException("Azure support is not available on this platform. Please rebuild with Azure SDK support.");
#endif

	return std::move(result);
}

unique_ptr<HTTPResponse> AzureAuthorization::Request(RequestType request_type, ClientContext &context,
                                                     const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
                                                     const string &data) {
	if (!token.empty()) {
		headers.Insert("Authorization", StringUtil::Format("Bearer %s", token));
	}
	return APIUtils::Request(request_type, context, endpoint_builder, this->client, headers, data);
}

void AzureAuthorization::SetCatalogSecretParameters(CreateSecretFunction &function) {
	auto &options = IcebergAzureSecretOptions();
	function.named_parameters.insert(options.begin(), options.end());
}

} // namespace duckdb
