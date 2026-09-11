#pragma once

#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/main/client_context.hpp"
#include "catalog/rest/storage/iceberg_authorization.hpp"

namespace duckdb {

//! A SigV4-signed request to an AWS-hosted Iceberg REST catalog. Signing is done here with
//! duckdb_mbedtls and the request goes out over duckdb's HTTPUtil, so this needs nothing from
//! aws-sdk-cpp.
class AWSInput {
public:
	AWSInput(AttachedDatabase &db) : attached_db(db) {
	}

public:
	unique_ptr<HTTPResponse> Request(RequestType request_type, ClientContext &context, HTTPHeaders &headers,
	                                 const string &data);

	//! The path that goes into the SigV4 canonical request. Mirrors
	//! Aws::Http::URI::GetURLEncodedPath: every byte outside A-Za-z0-9-_.~ is percent-encoded,
	//! and no segments at all yields "/".
	string CanonicalPath() const;
	//! The path that goes on the wire. Mirrors Aws::Http::URI::GetURLEncodedPathRFC3986 in the
	//! non-RFC mode the SDK defaults to, which additionally leaves $ & , : = @ unescaped -- and
	//! mirrors GetURIString in emitting nothing at all when there are no segments.
	//!
	//! That this differs from CanonicalPath() is the SDK's behaviour, not an oversight here:
	//! iceberg has always signed one encoding and sent the other. Changing it is a separate
	//! decision from removing the SDK, so it is preserved verbatim.
	string WirePath() const;
	//! "" when there are no parameters, otherwise "?k=v&k2=v2" in insertion order. Callers must
	//! add parameters in the order SigV4 signs them; nothing sorts them here. Both the wire URL
	//! and the canonical request use this same string, as they did with the SDK.
	string QueryString() const;
	//! The URL to send to: scheme, authority, wire path, query string.
	string URL() const;

public:
	AttachedDatabase &attached_db;
	//! The scheme to use for this request, defaults to HTTPS
	bool use_https = true;
	string authority;
	vector<string> path_segments;
	vector<std::pair<string, string>> query_string_parameters;

	//! Provider credentials
	string key_id;
	string secret;
	string session_token;
	//! Signer input
	string service;
	string region;
};

} // namespace duckdb
