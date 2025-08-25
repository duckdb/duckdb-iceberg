#pragma once

#include "storage/irc_authorization.hpp"
#include "aws.hpp"

namespace duckdb {

class SIGV4Authorization : public IRCAuthorization {
public:
	static constexpr const IRCAuthorizationType TYPE = IRCAuthorizationType::SIGV4;

public:
	SIGV4Authorization();
	SIGV4Authorization(const string &secret);

public:
	static unique_ptr<IRCAuthorization> FromAttachOptions(IcebergAttachOptions &input);
	unique_ptr<HTTPResponse> GetRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder) override;
	unique_ptr<HTTPResponse> DeleteRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder) override;
	unique_ptr<HTTPResponse> PostRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder,
	                                     const string &body) override;

private:
	AWSInput CreateAWSInput(ClientContext &context, const IRCEndpointBuilder &endpoint_builder);

public:
	string secret;
	string region;
};

} // namespace duckdb
