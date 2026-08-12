#pragma once

#include "catalog/rest/storage/iceberg_authorization.hpp"
#include "catalog/rest/storage/aws.hpp"

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/thread_annotation.hpp"

#include <chrono>

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
	//! Refresh this catalog's S3 secret if it has refresh_info and the interval has elapsed.
	void MaybeRefreshSecret(ClientContext &context);

public:
	string secret;
	string region;
	//! Optional: override the AWS service name used for SigV4 signing, useful for self-hosted REST catalog services
	string sigv4_service;
	//! Optional: override the AWS region used for SigV4 signing, useful for non-AWS endpoints
	string sigv4_region;

private:
	//! Per-instance, so catalogs with different secrets refresh independently.
	annotated_mutex refresh_mutex;
	//! Set to construction time, not the epoch: the secret was just created, so an
	//! immediate refresh would be a redundant STS call.
	std::chrono::steady_clock::time_point
	    last_refresh_time DUCKDB_GUARDED_BY(refresh_mutex) = std::chrono::steady_clock::now();
	//! STS tokens last at least 900s, so 300s leaves headroom.
	static constexpr int REFRESH_INTERVAL_SECONDS = 300;
};

} // namespace duckdb
