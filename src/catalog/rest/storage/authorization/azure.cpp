#include "catalog/rest/storage/authorization/azure.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include "catalog/rest/api/api_utils.hpp"

namespace duckdb {

namespace {

//! Name of the secret created by 'CREATE SECRET (TYPE azure, ...)' without a name
const char *DEFAULT_AZURE_SECRET = "__default_azure";

unique_ptr<SecretEntry> GetAzureSecret(ClientContext &context, const string &secret_name) {
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto secret_entry = context.db->GetSecretManager().GetSecretByName(transaction, secret_name);
	if (!secret_entry) {
		if (secret_name == DEFAULT_AZURE_SECRET) {
			throw InvalidConfigurationException(
			    "AUTHORIZATION_TYPE 'azure' requires a 'secret' of type 'azure', created with PROVIDER "
			    "credential_chain and REFRESH 'auto'");
		}
		throw InvalidConfigurationException(
		    "No secret by the name of '%s' could be found, consider changing the 'secret'", secret_name);
	}
	auto secret_type = secret_entry->secret->GetType().GetIdentifierName();
	if (!StringUtil::CIEquals(secret_type, "azure")) {
		throw InvalidConfigurationException(
		    "Secret '%s' is of type '%s', AUTHORIZATION_TYPE 'azure' requires a secret of type 'azure'", secret_name,
		    secret_type);
	}
	auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
	if (kv_secret.TryGetValue("token").IsNull()) {
		throw InvalidConfigurationException(
		    "Azure secret '%s' holds no token, create it with PROVIDER credential_chain and REFRESH 'auto'",
		    secret_name);
	}
	return secret_entry;
}

string GetSecretToken(const SecretEntry &secret_entry) {
	return dynamic_cast<const KeyValueSecret &>(*secret_entry.secret).TryGetValue("token").ToString();
}

bool TokenNeedsRefresh(ClientContext &context, const SecretEntry &secret_entry, int64_t margin_ms) {
	if (IcebergAuthorization::ForceTokenExpiry(context)) {
		return true;
	}
	auto expiration = dynamic_cast<const KeyValueSecret &>(*secret_entry.secret).TryGetValue("expiration_epoch_ms");
	if (expiration.IsNull()) {
		return false;
	}
	auto now_ms = Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp());
	return now_ms >= expiration.GetValue<int64_t>() - margin_ms;
}

} // namespace

AzureAuthorization::AzureAuthorization(AttachedDatabase &db)
    : IcebergAuthorization(db, IcebergAuthorizationType::AZURE) {
}

unique_ptr<IcebergAuthorization> AzureAuthorization::FromAttachOptions(AttachedDatabase &db, ClientContext &context,
                                                                       IcebergAttachOptions &input) {
	auto result = make_uniq<AzureAuthorization>(db);

	unordered_map<string, Value> remaining_options;
	for (auto &entry : input.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "secret") {
			if (!result->secret.empty()) {
				throw InvalidInputException("Duplicate 'secret' option detected!");
			}
			result->secret = StringUtil::Lower(entry.second.ToString());
		} else if (lower_name == "extra_http_headers") {
			IcebergAuthorization::ParseExtraHttpHeaders(entry.second, result->extra_http_headers);
		} else {
			remaining_options.emplace(std::move(entry));
		}
	}
	if (result->secret.empty()) {
		result->secret = DEFAULT_AZURE_SECRET;
	}
	// Fail at ATTACH rather than on the first catalog request
	(void)GetAzureSecret(context, result->secret);

	input.options = std::move(remaining_options);
	return std::move(result);
}

string AzureAuthorization::GetToken(ClientContext &context, bool force_refresh) {
	auto secret_entry = GetAzureSecret(context, secret);
	if (!force_refresh && !TokenNeedsRefresh(context, *secret_entry, REFRESH_MARGIN_MS)) {
		return GetSecretToken(*secret_entry);
	}

	annotated_lock_guard<annotated_mutex> lock(refresh_mutex);
	auto current_entry = GetAzureSecret(context, secret);
	auto current_token = GetSecretToken(*current_entry);
	if (current_token != GetSecretToken(*secret_entry)) {
		// Another thread refreshed while we waited for the lock
		return current_token;
	}
	try {
		if (!IcebergAuthorization::ReplaySecretRefresh(context, *current_entry)) {
			// No refresh_info, the current token is all we have
			return current_token;
		}
	} catch (std::exception &ex) {
		if (force_refresh) {
			throw;
		}
		// The token is still valid for a while, so keep using it and retry on the next request
		DUCKDB_LOG_DEBUG(context, "Iceberg Azure secret '%s' was not updated, refresh failed: %s", secret, ex.what());
		return current_token;
	}
	return GetSecretToken(*GetAzureSecret(context, secret));
}

unique_ptr<HTTPResponse> AzureAuthorization::Request(RequestType request_type, ClientContext &context,
                                                     const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
                                                     const string &data) {
	for (auto &entry : extra_http_headers) {
		headers.Insert(entry.first, entry.second);
	}
	headers["Authorization"] = StringUtil::Format("Bearer %s", GetToken(context, false));
	auto response = APIUtils::Request(request_type, context, endpoint_builder, headers, data);

	// The token can be rejected before it expires (revoked, clock skew), re-run the credential chain once
	if (response->status == HTTPStatusCode::Unauthorized_401) {
		headers["Authorization"] = StringUtil::Format("Bearer %s", GetToken(context, true));
		response = APIUtils::Request(request_type, context, endpoint_builder, headers, data);
	}
	return response;
}

} // namespace duckdb
