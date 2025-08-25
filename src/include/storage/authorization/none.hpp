#pragma once

#include "storage/irc_authorization.hpp"

namespace duckdb {

class NoneAuthorization : public IRCAuthorization {
public:
	static constexpr const IRCAuthorizationType TYPE = IRCAuthorizationType::NONE;

public:
	NoneAuthorization();

public:
	static unique_ptr<IRCAuthorization> FromAttachOptions(IcebergAttachOptions &input);
	unique_ptr<HTTPResponse> GetRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder) override;
	unique_ptr<HTTPResponse> DeleteRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder) override;
	unique_ptr<HTTPResponse> PostRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder,
	                                     const string &body) override;
};

} // namespace duckdb
