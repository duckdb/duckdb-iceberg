#pragma once

#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/main/client_context.hpp"
#include "storage/irc_authorization.hpp"

#ifdef EMSCRIPTEN
#else
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/http/HttpRequest.h>
#endif

namespace duckdb {

class AWSInput {
public:
	AWSInput() {
	}

public:
	unique_ptr<HTTPResponse> Request(RequestType request_type, ClientContext &context, HTTPHeaders &headers,
	                                 const string &data);

#ifdef EMSCRIPTEN
#else
	unique_ptr<HTTPResponse> ExecuteRequest(ClientContext &context, Aws::Http::HttpMethod method, HTTPHeaders &headers,
	                                        const string &body = "");
	std::shared_ptr<Aws::Http::HttpRequest> CreateSignedRequest(Aws::Http::HttpMethod method, const Aws::Http::URI &uri,
	                                                            HTTPHeaders &headers, const string &body = "");
	Aws::Http::URI BuildURI();
	Aws::Client::ClientConfiguration BuildClientConfig();
#endif

public:
	//! NOTE: 'scheme' is assumed to be HTTPS!
	string authority;
	vector<string> path_segments;
	vector<std::pair<string, string>> query_string_parameters;
	string user_agent;
	string cert_path;
	bool use_httpfs_timeout = false;
	idx_t request_timeout_in_ms;

	//! Provider credentials
	string key_id;
	string secret;
	string session_token;
	//! Signer input
	string service;
	string region;
};

} // namespace duckdb
