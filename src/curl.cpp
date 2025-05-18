#include "curl.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception/http_exception.hpp"

#ifdef WASM_LOADABLE_EXTENSIONS
#else
#include <curl/curl.h>
#endif

namespace duckdb {

#ifdef WASM_LOADABLE_EXTENSIONS

string RequestInput::GetRequest(ClientContext &context) {
	throw NotImplementedException("GET on WASM not implemented yet");
}

string RequestInput::PostRequest(ClientContext &context, const string &post_data) {
	throw NotImplementedException("POST on WASM not implemented yet");
}

string RequestInput::DeleteRequest(ClientContext &context) {
	throw NotImplementedException("DELETE on WASM not implemented yet");
}

#else

namespace {

class CURLHandle {
public:
	CURLHandle(const string &token, const string &cert_path) {
		curl = curl_easy_init();
		if (!curl) {
			throw InternalException("Failed to initialize curl");
		}
		if (!token.empty()) {
			curl_easy_setopt(curl, CURLOPT_XOAUTH2_BEARER, token.c_str());
			curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BEARER);
		}
		if (!cert_path.empty()) {
			curl_easy_setopt(curl, CURLOPT_CAINFO, cert_path.c_str());
		}
	}
	~CURLHandle() {
		curl_easy_cleanup(curl);
	}

public:
	operator CURL *() {
		return curl;
	}
	CURLcode Execute() {
		return curl_easy_perform(curl);
	}

private:
	CURL *curl = NULL;
};

class CURLRequestHeaders {
public:
	CURLRequestHeaders(const vector<string> &input) {
		for (auto &header : input) {
			Add(header);
		}
	}
	~CURLRequestHeaders() {
		if (headers) {
			curl_slist_free_all(headers);
		}
		headers = NULL;
	}
	operator bool() const {
		return headers != NULL;
	}

public:
	void Add(const string &header) {
		headers = curl_slist_append(headers, header.c_str());
	}

public:
	curl_slist *headers = NULL;
};

static size_t RequestWriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
	((std::string *)userp)->append((char *)contents, size * nmemb);
	return size * nmemb;
}

} // namespace

string RequestInput::DeleteRequest(ClientContext &context) {
	CURLRequestHeaders curl_headers(headers);

	CURLcode res;
	string result;
	{
		//! Put in inner scope, cleanup happens in the destructor
		CURLHandle curl(bearer_token, cert_path);
		curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
		curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, RequestWriteCallback);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result);

		if (curl_headers) {
			curl_easy_setopt(curl, CURLOPT_HTTPHEADER, curl_headers.headers);
		}
		res = curl.Execute();
	}

	DUCKDB_LOG_DEBUG(context, "iceberg.Catalog.Curl.HTTPRequest", "DELETE %s (curl code '%s')", url,
	                 curl_easy_strerror(res));
	if (res != CURLcode::CURLE_OK) {
		string error = curl_easy_strerror(res);
		throw HTTPException(StringUtil::Format("Curl DELETE Request to '%s' failed with error: '%s'", url, error));
	}
	return result;
}

string RequestInput::PostRequest(ClientContext &context, const string &post_data) {
	CURLRequestHeaders curl_headers(headers);

	CURLcode res;
	string result;
	{
		//! Put in inner scope, cleanup happens in the destructor
		CURLHandle curl(bearer_token, cert_path);
		curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
		curl_easy_setopt(curl, CURLOPT_POST, 1L);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_data.c_str());
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, RequestWriteCallback);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result);

		if (curl_headers) {
			curl_easy_setopt(curl, CURLOPT_HTTPHEADER, curl_headers.headers);
		}
		res = curl.Execute();
	}

	DUCKDB_LOG_DEBUG(context, "iceberg.Catalog.Curl.HTTPRequest", "POST %s (curl code '%s')", url,
	                 curl_easy_strerror(res));
	if (res != CURLcode::CURLE_OK) {
		string error = curl_easy_strerror(res);
		throw HTTPException(StringUtil::Format("Curl POST Request to '%s' failed with error: '%s'", url, error));
	}
	return result;
}

string RequestInput::GetRequest(ClientContext &context) {
	CURLRequestHeaders curl_headers(headers);

	CURLcode res;
	string result;
	{
		CURLHandle curl(bearer_token, cert_path);
		curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, RequestWriteCallback);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result);

		if (curl_headers) {
			curl_easy_setopt(curl, CURLOPT_HTTPHEADER, curl_headers.headers);
		}
		res = curl.Execute();
	}

	DUCKDB_LOG_DEBUG(context, "iceberg.Catalog.Curl.HTTPRequest", "GET %s (curl code '%s')", url,
	                 curl_easy_strerror(res));
	if (res != CURLcode::CURLE_OK) {
		string error = curl_easy_strerror(res);
		throw HTTPException(StringUtil::Format("Curl GET Request to '%s' failed with error: '%s'", url, error));
	}
	return result;
}

#endif

} // namespace duckdb
