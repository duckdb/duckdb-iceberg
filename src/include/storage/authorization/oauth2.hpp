#pragma once

#include "storage/irc_authorization.hpp"

namespace duckdb {

class OAuth2Authorization : public IRCAuthorization {
public:
	static constexpr const IRCAuthorizationType TYPE = IRCAuthorizationType::OAUTH2;

public:
	OAuth2Authorization();
	OAuth2Authorization(const string &grant_type, const string &uri, const string &client_id,
	                    const string &client_secret, const string &scope);

public:
	static unique_ptr<OAuth2Authorization> FromAttachOptions(ClientContext &context, IcebergAttachOptions &input);
	unique_ptr<HTTPResponse> GetRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder) override;
	unique_ptr<HTTPResponse> HeadRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder) override;
	unique_ptr<HTTPResponse> DeleteRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder) override;
	unique_ptr<HTTPResponse> PostRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder,
	                                     const string &body) override;
	static string GetToken(ClientContext &context, const string &grant_type, const string &uri, const string &client_id,
	                       const string &client_secret, const string &scope);
	static void SetCatalogSecretParameters(CreateSecretFunction &function);
	static unique_ptr<BaseSecret> CreateCatalogSecretFunction(ClientContext &context, CreateSecretInput &input);

public:
	string grant_type;
	string uri;
	string client_id;
	string client_secret;
	string scope;

	//! The user-supplied default region to add to the default secret
	string default_region;

	//! The (bearer) token retrieved
	string token;
};

} // namespace duckdb
