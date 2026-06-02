#pragma once

#include "catalog/rest/storage/iceberg_authorization.hpp"
#include "catalog/rest/storage/aws.hpp"

namespace duckdb {

class SIGV4Authorization : public IcebergAuthorization {
public:
	static constexpr const IcebergAuthorizationType TYPE = IcebergAuthorizationType::SIGV4;

public:
	SIGV4Authorization(AttachedDatabase &db);
	SIGV4Authorization(AttachedDatabase &db, const string &secret);

public:
	static unique_ptr<IcebergAuthorization> FromAttachOptions(AttachedDatabase &db, IcebergAttachOptions &input);
	unique_ptr<HTTPResponse> Request(RequestType request_type, ClientContext &context,
	                                 const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
	                                 const string &data = "") override;

private:
	AWSInput CreateAWSInput(ClientContext &context, const IRCEndpointBuilder &endpoint_builder);

public:
	string secret;
	string region;
	//! Optional: override the AWS service name used for SigV4 signing, useful for self-hosted REST catalog services
	string sigv4_service;
	//! Optional: override the AWS region used for SigV4 signing, useful for non-AWS endpoints
	string sigv4_region;
};

} // namespace duckdb
