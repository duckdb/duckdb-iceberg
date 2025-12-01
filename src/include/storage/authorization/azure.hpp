#pragma once

#include "storage/irc_authorization.hpp"

namespace duckdb {

class AzureAuthorization : public IRCAuthorization {
public:
	static constexpr const IRCAuthorizationType TYPE = IRCAuthorizationType::AZURE;

public:
	AzureAuthorization();
	AzureAuthorization(const string &account_name, const string &chain);

public:
	static unique_ptr<AzureAuthorization> FromAttachOptions(ClientContext &context, IcebergAttachOptions &input);
	unique_ptr<HTTPResponse> Request(RequestType request_type, ClientContext &context,
	                                 const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
	                                 const string &data = "") override;
	static void SetCatalogSecretParameters(CreateSecretFunction &function);
	static unique_ptr<BaseSecret> CreateCatalogSecretFunction(ClientContext &context, CreateSecretInput &input);

public:
	string account_name;
	string chain;
	//! The (bearer) token retrieved from Azure CLI or other credential chain
	string token;
};

} // namespace duckdb
