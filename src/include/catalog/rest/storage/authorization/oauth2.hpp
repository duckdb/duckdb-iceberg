#pragma once

#include "catalog/rest/storage/iceberg_authorization.hpp"
#include <mutex>

namespace duckdb {

enum class OAuth2GrantType : uint8_t { CLIENT_CREDENTIALS, REFRESH_TOKEN };

struct OAuth2Credentials {
	explicit OAuth2Credentials(OAuth2GrantType grant_type) : grant_type(grant_type) {
	}
	virtual ~OAuth2Credentials() = default;

	template <class TARGET>
	const TARGET &Cast() const {
		if (grant_type != TARGET::GRANT_TYPE) {
			throw InternalException("OAuth2 credentials grant type mismatch");
		}
		return static_cast<const TARGET &>(*this);
	}

	const OAuth2GrantType grant_type;
};

struct ClientCredentials : public OAuth2Credentials {
	static constexpr auto GRANT_TYPE = OAuth2GrantType::CLIENT_CREDENTIALS;

	ClientCredentials(const string &client_id, const string &client_secret)
	    : OAuth2Credentials(GRANT_TYPE), client_id(client_id), client_secret(client_secret) {
	}

	const string client_id;
	const string client_secret;
};

struct RefreshTokenCredentials : public OAuth2Credentials {
	static constexpr auto GRANT_TYPE = OAuth2GrantType::REFRESH_TOKEN;

	RefreshTokenCredentials(const ClientCredentials &client_credentials, const string &refresh_token)
	    : OAuth2Credentials(GRANT_TYPE), client_credentials(client_credentials), refresh_token(refresh_token) {
	}

	//! Required client authentication for the supported refresh-token flow.
	const ClientCredentials client_credentials;
	const string refresh_token;
};

class OAuth2Authorization : public IcebergAuthorization {
public:
	static constexpr const IcebergAuthorizationType TYPE = IcebergAuthorizationType::OAUTH2;

public:
	OAuth2Authorization(AttachedDatabase &db);
	OAuth2Authorization(AttachedDatabase &db, unique_ptr<const OAuth2Credentials> credentials, const string &uri,
	                    const string &scope, const string &default_region = "");

public:
	static unique_ptr<OAuth2Authorization> FromAttachOptions(AttachedDatabase &db, ClientContext &context,
	                                                         IcebergAttachOptions &input);
	unique_ptr<HTTPResponse> Request(RequestType request_type, ClientContext &context,
	                                 const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
	                                 const string &data = "") override;
	static string GetToken(ClientContext &context, const OAuth2Credentials &credentials, const string &uri,
	                       const string &scope);
	static void SetCatalogSecretParameters(CreateSecretFunction &function);
	static unique_ptr<BaseSecret> CreateCatalogSecretFunction(ClientContext &context, CreateSecretInput &input);

public:
	//! OAuth2 configuration (set during construction, immutable after that)
	const string uri;
	const string scope;
	const string default_region;

private:
	//! Token state and grant credentials (protected by token_mutex)
	string token;
	//! Null for a token-only configuration; credential values may be empty.
	unique_ptr<const OAuth2Credentials> credentials;
	int64_t token_expires_at = 0;
	int32_t last_expires_in = 0;

	//! Helper to update token state from OAuth2 response.
	//! Safe to call during construction (before sharing) and under token_mutex afterwards.
	void UpdateTokenState(const string &new_token, int32_t expires_in, const string &new_refresh_token);

	//! Internal methods -- caller must hold token_mutex
	bool IsTokenExpiredUnlocked(ClientContext &context, const std::lock_guard<std::mutex> &lock) const;
	bool CanRefreshUnlocked(const std::lock_guard<std::mutex> &lock) const;
	void RefreshAccessTokenUnlocked(ClientContext &context, const std::lock_guard<std::mutex> &lock);

	//! Mutex to serialize token refresh. Held during check+refresh+copy, released before catalog I/O.
	//! At most one thread refreshes at a time; others queue and re-check expiry after acquiring.
	std::mutex token_mutex;
};

} // namespace duckdb
