#include "api_utils.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <sys/stat.h>

namespace duckdb {
//! Grab the first path that exists, from a list of well-known locations
static string SelectCURLCertPath() {
	for (string &caFile : certFileLocations) {
		struct stat buf;
		if (stat(caFile.c_str(), &buf) == 0) {
			return caFile;
		}
	}
	return string();
}

const string &APIUtils::GetCURLCertPath() {
	static string cert_path = SelectCURLCertPath();
	return cert_path;
}

unique_ptr<HTTPResponse> APIUtils::Request(RequestType request_type, ClientContext &context,
                                           const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
                                           const string &data) {
	auto &db = DatabaseInstance::GetDatabase(context);
	string request_url = AddHttpHostIfMissing(endpoint_builder.GetURL());

	auto &http_util = HTTPUtil::Get(db);
	unique_ptr<HTTPParams> params;
	params = http_util.InitializeParameters(context, request_url);

	switch (request_type) {
	case RequestType::GET_REQUEST: {
		GetRequestInfo get_request(request_url, headers, *params, nullptr, nullptr);
		return http_util.Request(get_request);
	}
	case RequestType::DELETE_REQUEST: {
		DeleteRequestInfo delete_request(request_url, headers, *params);
		return http_util.Request(delete_request);
	}
	case RequestType::POST_REQUEST: {
		PostRequestInfo post_request(request_url, headers, *params, reinterpret_cast<const_data_ptr_t>(data.data()),
		                             data.size());
		auto response = http_util.Request(post_request);
		response->body = post_request.buffer_out;
		return response;
	}
	case RequestType::HEAD_REQUEST: {
		HeadRequestInfo head_request(request_url, headers, *params);
		return http_util.Request(head_request);
	}
	default:
		throw NotImplementedException("Cannot make request of type %s", EnumUtil::ToString(request_type));
	}
}

} // namespace duckdb
