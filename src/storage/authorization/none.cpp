#include "storage/authorization/none.hpp"
#include "api_utils.hpp"
#include "storage/irc_catalog.hpp"

namespace duckdb {

NoneAuthorization::NoneAuthorization() : IRCAuthorization(IRCAuthorizationType::NONE) {
}

unique_ptr<IRCAuthorization> NoneAuthorization::FromAttachOptions(IcebergAttachOptions &input) {
	auto result = make_uniq<NoneAuthorization>();
	return std::move(result);
}

unique_ptr<HTTPResponse> NoneAuthorization::GetRequest(ClientContext &context,
                                                       const IRCEndpointBuilder &endpoint_builder) {
	return APIUtils::GetRequest(context, endpoint_builder, "");
}

unique_ptr<HTTPResponse> NoneAuthorization::HeadRequest(ClientContext &context,
                                                        const IRCEndpointBuilder &endpoint_builder) {
	return APIUtils::HeadRequest(context, endpoint_builder, "");
}

unique_ptr<HTTPResponse> NoneAuthorization::DeleteRequest(ClientContext &context,
                                                          const IRCEndpointBuilder &endpoint_builder) {
	return APIUtils::DeleteRequest(context, endpoint_builder, "");
}

unique_ptr<HTTPResponse>
NoneAuthorization::PostRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder, const string &body) {
	auto url = endpoint_builder.GetURL();
	unordered_map<string, string> empty_headers;
	return APIUtils::PostRequest(context, url, body, empty_headers, "json", "");
}

} // namespace duckdb
