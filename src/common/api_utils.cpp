#include "api_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_data.hpp"

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

static string AddHttpHostIfMissing(const string &url) {
	auto lower_url = StringUtil::Lower(url);
	if (StringUtil::StartsWith(lower_url, "http://") || StringUtil::StartsWith(lower_url, "https://")) {
		return url;
	}
	return "http://" + url;
}

unique_ptr<HTTPResponse> APIUtils::DeleteRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder,
                                                 const string &token) {
	auto &db = DatabaseInstance::GetDatabase(context);

	HTTPHeaders headers(db);
	headers.Insert("X-Iceberg-Access-Delegation", "vended-credentials");
	headers.Insert("Authorization", StringUtil::Format("Bearer %s", token));

	string request_url = AddHttpHostIfMissing(endpoint_builder.GetURL());
	auto &http_util = HTTPUtil::Get(db);
	unique_ptr<HTTPParams> params;
	params = http_util.InitializeParameters(context, request_url);

	DeleteRequestInfo delete_request(request_url, headers, *params);
	return http_util.Request(delete_request);
}

unique_ptr<HTTPResponse> APIUtils::PostRequest(ClientContext &context, const string &url, const string &post_data,
                                               const unordered_map<string, string> &additional_headers,
                                               const string &content_type, const string &token) {
	auto &db = DatabaseInstance::GetDatabase(context);

	string request_url = AddHttpHostIfMissing(url);

	HTTPHeaders headers(db);
	headers.Insert("X-Iceberg-Access-Delegation", "vended-credentials");
	headers.Insert("Content-Type", StringUtil::Format("application/%s", content_type));
	for (auto it : additional_headers) {
		headers.Insert(it.first, it.second);
	}
	if (!token.empty()) {
		headers.Insert("Authorization", StringUtil::Format("Bearer %s", token));
	}

	auto &http_util = HTTPUtil::Get(db);
	unique_ptr<HTTPParams> params;
	params = http_util.InitializeParameters(context, request_url);

	PostRequestInfo post_request(request_url, headers, *params, reinterpret_cast<const_data_ptr_t>(post_data.data()),
	                             post_data.size());
	auto response = http_util.Request(post_request);
	response->body = post_request.buffer_out;
	return response;
}

unique_ptr<HTTPResponse> APIUtils::GetRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder,
                                              const string &token) {
	auto &db = DatabaseInstance::GetDatabase(context);

	HTTPHeaders headers(db);
	headers.Insert("X-Iceberg-Access-Delegation", "vended-credentials");
	if (!token.empty()) {
		headers.Insert("Authorization", StringUtil::Format("Bearer %s", token));
	}

	auto &http_util = HTTPUtil::Get(db);
	unique_ptr<HTTPParams> params;

	string request_url = AddHttpHostIfMissing(endpoint_builder.GetURL());

	params = http_util.InitializeParameters(context, request_url);

	GetRequestInfo get_request(request_url, headers, *params, nullptr, nullptr);
	return http_util.Request(get_request);
}

} // namespace duckdb
