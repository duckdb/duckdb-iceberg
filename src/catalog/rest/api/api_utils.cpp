#include "catalog/rest/api/api_utils.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/http_transport_manager.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

unique_ptr<HTTPResponse> APIUtils::Request(RequestType request_type, ClientContext &context,
                                           const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
                                           const string &data) {
	// load httpfs since iceberg requests do not go through the file system api
	if (!context.db.get()) {
		throw InvalidConfigurationException("Context does not have database instance when loading Httpfs in Iceberg");
	}
	ExtensionHelper::AutoLoadExtension(*context.db, "httpfs");
	if (!context.db->ExtensionIsLoaded("httpfs")) {
		throw MissingExtensionException("The iceberg extension requires the httpfs extension to be loaded!");
	}

	auto &db = DatabaseInstance::GetDatabase(context);
	string request_url = AddHttpHostIfMissing(endpoint_builder.GetURLEncoded());

	auto session = db.config.GetHTTPTransportManager().CreateSession(context, request_url);
	auto &params = session.Parameters();

	switch (request_type) {
	case RequestType::GET_REQUEST: {
		GetRequestInfo get_request(request_url, headers, params, nullptr, nullptr);
		return session.Request(get_request);
	}
	case RequestType::DELETE_REQUEST: {
		DeleteRequestInfo delete_request(request_url, headers, params);
		return session.Request(delete_request);
	}
	case RequestType::POST_REQUEST: {
		PostRequestInfo post_request(request_url, headers, params, reinterpret_cast<const_data_ptr_t>(data.data()),
		                             data.size());
		auto response = session.Request(post_request);
		response->body = post_request.buffer_out;
		return response;
	}
	case RequestType::HEAD_REQUEST: {
		HeadRequestInfo head_request(request_url, headers, params);
		return session.Request(head_request);
	}
	default:
		throw NotImplementedException("Cannot make request of type %s", EnumUtil::ToString(request_type));
	}
}

} // namespace duckdb
