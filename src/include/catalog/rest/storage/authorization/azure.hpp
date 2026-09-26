#pragma once

#include "catalog/rest/storage/iceberg_authorization.hpp"

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/thread_annotation.hpp"

namespace duckdb {

//! Bearer token authorization backed by a duckdb-azure secret, the Azure counterpart of SIGV4Authorization.
//! The secret (TYPE azure, PROVIDER credential_chain, REFRESH 'auto') holds the token, and is re-created from its
//! 'refresh_info' when the token is about to expire.
class AzureAuthorization : public IcebergAuthorization {
public:
	static constexpr const IcebergAuthorizationType TYPE = IcebergAuthorizationType::AZURE;

public:
	explicit AzureAuthorization(AttachedDatabase &db);

public:
	static unique_ptr<IcebergAuthorization> FromAttachOptions(AttachedDatabase &db, ClientContext &context,
	                                                          IcebergAttachOptions &input);
	unique_ptr<HTTPResponse> Request(RequestType request_type, ClientContext &context,
	                                 const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
	                                 const string &data = "") override;

private:
	//! The secret's token, re-creating the secret first if the token is about to expire (or 'force_refresh')
	string GetToken(ClientContext &context, bool force_refresh);

public:
	string secret;

private:
	//! Serializes refreshes, so concurrent requests re-run the credential chain once
	annotated_mutex refresh_mutex;
	//! Refresh this long before the token expires
	static constexpr int64_t REFRESH_MARGIN_MS = 5 * 60 * 1000;
};

} // namespace duckdb
