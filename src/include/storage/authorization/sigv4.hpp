#pragma once

#include "storage/irc_authorization.hpp"
#include "curl.hpp"

namespace duckdb {

class SIGV4Authorization : public IRCAuthorization {
public:
	static constexpr const IRCAuthorizationType TYPE = IRCAuthorizationType::SIGV4;

public:
	SIGV4Authorization();
	SIGV4Authorization(const string &secret);

public:
	static unique_ptr<IRCAuthorization> FromAttachOptions(IcebergAttachOptions &input);
	string GetRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder,
	                  RequestInput &request_input) override;

public:
	string secret;
	string region;
};

} // namespace duckdb
