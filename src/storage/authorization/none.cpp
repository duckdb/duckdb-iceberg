#include "storage/authorization/none.hpp"
#include "api_utils.hpp"
#include "storage/catalog/iceberg_catalog.hpp"

namespace duckdb {

NoneAuthorization::NoneAuthorization() : IcebergAuthorization(IcebergAuthorizationType::NONE) {
}

unique_ptr<IcebergAuthorization> NoneAuthorization::FromAttachOptions(IcebergAttachOptions &input) {
	auto result = make_uniq<NoneAuthorization>();
	return std::move(result);
}

unique_ptr<HTTPResponse> NoneAuthorization::Request(RequestType request_type, ClientContext &context,
                                                    const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
                                                    const string &data) {
	return APIUtils::Request(request_type, context, endpoint_builder, client, headers, data);
}

} // namespace duckdb
