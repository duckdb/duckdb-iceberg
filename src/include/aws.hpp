#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/main/client_context.hpp"

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
	unique_ptr<HTTPResponse> GetRequest(ClientContext &context);
	unique_ptr<HTTPResponse> DeleteRequest(ClientContext &context);
	unique_ptr<HTTPResponse> PostRequest(ClientContext &context, string post_body);

#ifdef EMSCRIPTEN
#else
	unique_ptr<HTTPResponse> ExecuteRequest(ClientContext &context, Aws::Http::HttpMethod method,
	                                        const string body = "", string content_type = "");
	std::shared_ptr<Aws::Http::HttpRequest> CreateSignedRequest(Aws::Http::HttpMethod method, const Aws::Http::URI &uri,
	                                                            const string &body = "", string content_type = "");
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

	//! Provider credentials
	string key_id;
	string secret;
	string session_token;
	//! Signer input
	string service;
	string region;
};

} // namespace duckdb
