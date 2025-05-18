#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

class AWSInput {
public:
	AWSInput() {
	}

public:
	string GetRequest(ClientContext &context);

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
