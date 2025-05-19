#include "api_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/string_util.hpp"

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

string APIUtils::DeleteRequest(ClientContext &context, const string &url, RequestInput &request_input,
                               const string &token) {
	// Set the user Agent.
	auto &config = DBConfig::GetConfig(context);
	request_input.AddHeader(StringUtil::Format("User-Agent: %s", config.UserAgent()));
	request_input.SetURL(url);
	request_input.SetCertPath(GetCURLCertPath());
	request_input.SetBearerToken(token);

	return request_input.DeleteRequest(context);
}

string APIUtils::PostRequest(ClientContext &context, const string &url, const string &post_data,
                             RequestInput &request_input, const string &content_type, const string &token) {
	auto &config = DBConfig::GetConfig(context);
	request_input.AddHeader(StringUtil::Format("User-Agent: %s", config.UserAgent()));
	request_input.AddHeader("Content-Type: application/" + content_type);
	request_input.SetURL(url);
	request_input.SetCertPath(GetCURLCertPath());
	request_input.SetBearerToken(token);

	return request_input.PostRequest(context, post_data);
}

string APIUtils::GetRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder,
                            RequestInput &request_input, const string &token) {
	auto url = endpoint_builder.GetURL();
	// Set the user Agent.
	auto &config = DBConfig::GetConfig(context);
	request_input.AddHeader(StringUtil::Format("User-Agent: %s", config.UserAgent()));
	request_input.SetURL(url);
	request_input.SetCertPath(GetCURLCertPath());
	request_input.SetBearerToken(token);

	return request_input.GetRequest(context);
}

} // namespace duckdb
