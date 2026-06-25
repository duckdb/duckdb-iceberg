#pragma once

#include "catalog/rest/storage/iceberg_authorization.hpp"
#include "catalog/rest/storage/aws.hpp"

#include <chrono>
#include <mutex>

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

	//! Accessors for the shared refresh mutex and timestamp — used by TryRefreshStorageSecret
	//! which also calls CreateSecret on the same aws_secret.
	static std::mutex &GetRefreshMutex() { return refresh_mutex; }
	static std::chrono::steady_clock::time_point &GetLastRefreshTime() { return last_refresh_time; }

private:
	AWSInput CreateAWSInput(ClientContext &context, const IRCEndpointBuilder &endpoint_builder);

	//! Refresh the S3 secret if it has refresh_info and enough time has passed.
	//! Serialized by refresh_mutex so only one thread performs the refresh.
	void MaybeRefreshSecret(ClientContext &context);

public:
	string secret;
	string region;
	//! Optional: override the AWS service name used for SigV4 signing
	string sigv4_service;
	//! Optional: override the AWS region used for SigV4 signing
	string sigv4_region;

private:
	//! Guards ALL CreateSecret calls on aws_secret across the entire process.
	//! Shared between MaybeRefreshSecret and TryRefreshStorageSecret via accessors.
	static std::mutex refresh_mutex;
	//! Time of last refresh attempt (success or failure). Shared for same reason.
	static std::chrono::steady_clock::time_point last_refresh_time;
	//! Minimum seconds between refresh attempts.
	static constexpr int REFRESH_INTERVAL_SECONDS = 300;
};

} // namespace duckdb
