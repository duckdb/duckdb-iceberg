#include "storage/authorization/none.hpp"
#include "api_utils.hpp"
#include "storage/catalog/iceberg_catalog.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

NoneAuthorization::NoneAuthorization() : IcebergAuthorization(IcebergAuthorizationType::NONE) {
}

unique_ptr<IcebergAuthorization> NoneAuthorization::FromAttachOptions(IcebergAttachOptions &input) {
	auto result = make_uniq<NoneAuthorization>();

	// Parse extra_http_headers if provided directly in attach options
	if (input.options.count("extra_http_headers")) {
		IcebergAuthorization::ParseExtraHttpHeaders(input.options["extra_http_headers"], result->extra_http_headers);
		input.options.erase("extra_http_headers");
	}

	return std::move(result);
}

unique_ptr<HTTPResponse> NoneAuthorization::Request(RequestType request_type, ClientContext &context,
                                                    const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
                                                    const string &data) {
	// Merge extra HTTP headers
	for (auto &entry : extra_http_headers) {
		headers.Insert(entry.first, entry.second);
	}

	return APIUtils::Request(request_type, context, endpoint_builder, client, headers, data);
}

} // namespace duckdb
