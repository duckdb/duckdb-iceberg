#pragma once

#include "storage/iceberg_authorization.hpp"
#include <mutex>

namespace duckdb {

class OAuth2Authorization : public IcebergAuthorization {
public:
	static constexpr const IcebergAuthorizationType TYPE = IcebergAuthorizationType::OAUTH2;

public:
	OAuth2Authorization();
	OAuth2Authorization(const string &grant_type, const string &uri, const string &client_id,
	                    const string &client_secret, const string &scope);

public:
	static unique_ptr<OAuth2Authorization> FromAttachOptions(ClientContext &context, IcebergAttachOptions &input);
	unique_ptr<HTTPResponse> Request(RequestType request_type, ClientContext &context,
	                                 const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
	                                 const string &data = "") override;
	static string GetToken(ClientContext &context, const string &grant_type, const string &uri, const string &client_id,
	                       const string &client_secret, const string &scope);
	static void SetCatalogSecretParameters(CreateSecretFunction &function);
	static unique_ptr<BaseSecret> CreateCatalogSecretFunction(ClientContext &context, CreateSecretInput &input);

public:
	//! OAuth2 configuration (set during construction, immutable after that)
	string grant_type;
	string uri;
	string client_id;
	string client_secret;
	string scope;
	string default_region;

private:
	//! Mutable token state (protected by token_mutex)
	string token;
	string refresh_token;
	int64_t token_expires_at = 0;
	int32_t last_expires_in = 0;

	//! Helper to update token state from OAuth2 response.
	//! Safe to call during construction (before sharing) and under token_mutex afterwards.
	void UpdateTokenState(const string &new_token, int32_t expires_in, const string &new_refresh_token);

	//! Internal methods -- caller must hold token_mutex
	bool IsTokenExpiredUnlocked(ClientContext &context, std::lock_guard<std::mutex> &lock) const;
	bool CanRefreshUnlocked(std::lock_guard<std::mutex> &lock) const;
	void RefreshAccessTokenUnlocked(ClientContext &context, std::lock_guard<std::mutex> &lock);

	//! Mutex to serialize token refresh. Held during check+refresh+copy, released before catalog I/O.
	//! At most one thread refreshes at a time; others queue and re-check expiry after acquiring.
	mutable std::mutex token_mutex;
};

} // namespace duckdb
