//===----------------------------------------------------------------------===//
//                         DuckDB
//
// api_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/printer.hpp"
#include "yyjson.hpp"
#include "duckdb/common/file_system.hpp"
#include "url_utils.hpp"

using namespace duckdb_yyjson;

//! fwd declare
struct curl_slist;
typedef void CURL;

namespace duckdb {

static string SELECTED_CURL_CERT_PATH = "";

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
	//! We use a global here to store the path that is selected on the ICAPI::InitializeCurl call

	static string GetRequestAws(ClientContext &context, IRCEndpointBuilder endpoint_builder, const string &secret_name);
	static string GetAwsRegion(const string &host);
	static string GetAwsService(const string &host);
	static string GetRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder,
	                         const string &token = "", curl_slist *extra_headers = NULL);
	static string DeleteRequest(const string &url, const string &token = "", curl_slist *extra_headers = NULL);
	static void InitializeCurlObject(CURL *curl, const string &token);
	static bool SetCurlCAFileInfo(CURL *curl);
	static bool SelectCurlCertPath();
	static size_t RequestWriteCallback(void *contents, size_t size, size_t nmemb, void *userp);
	static string PostRequest(ClientContext &context, const string &url, const string &post_data,
	                          const string &content_type = "x-www-form-urlencoded", const string &token = "",
	                          curl_slist *extra_headers = NULL);
};

} // namespace duckdb
