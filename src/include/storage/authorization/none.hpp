#pragma once

#include "storage/iceberg_authorization.hpp"

namespace duckdb {

class NoneAuthorization : public IcebergAuthorization {
public:
	static constexpr const IcebergAuthorizationType TYPE = IcebergAuthorizationType::NONE;

public:
	NoneAuthorization();

public:
	static unique_ptr<IcebergAuthorization> FromAttachOptions(IcebergAttachOptions &input);
	unique_ptr<HTTPResponse> Request(RequestType request_type, ClientContext &context,
	                                 const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
	                                 const string &data = "") override;
};

} // namespace duckdb
