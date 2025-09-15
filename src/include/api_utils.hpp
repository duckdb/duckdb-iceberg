//===----------------------------------------------------------------------===//
//                         DuckDB
//
// api_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/printer.hpp"
#include "duckdb/common/file_system.hpp"
#include "url_utils.hpp"
#include "aws.hpp"
#include "duckdb/common/http_util.hpp"

namespace duckdb {

// we statically compile in libcurl, which means the cert file location of the build machine is the
// place curl will look. But not every distro has this file in the same location, so we search a
// number of common locations and use the first one we find.
static string certFileLocations[] = {
    // Arch, Debian-based, Gentoo
    "/etc/ssl/certs/ca-certificates.crt",
    // RedHat 7 based
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    // Redhat 6 based
    "/etc/pki/tls/certs/ca-bundle.crt",
    // OpenSUSE
    "/etc/ssl/ca-bundle.pem",
    // Alpine
    "/etc/ssl/cert.pem"};

class APIUtils {
public:
	static unique_ptr<HTTPResponse> GetRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder,
	                                           const string &token = "");
	static unique_ptr<HTTPResponse> HeadRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder,
	                                            const string &token = "");
	static unique_ptr<HTTPResponse> DeleteRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder,
	                                              const string &token = "");
	static unique_ptr<HTTPResponse> PostRequest(ClientContext &context, const string &url, const string &post_data,
	                                            const unordered_map<string, string> &additional_headers,
	                                            const string &content_type = "x-www-form-urlencoded",
	                                            const string &token = "");
	//! We use a singleton here to store the path, set by SelectCurlCertPath
	static const string &GetCURLCertPath();
};

} // namespace duckdb
