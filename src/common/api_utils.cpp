#include "api_utils.hpp"
#include <sys/stat.h>
#include "storage/irc_catalog.hpp"
#include "credentials/credential_provider.hpp"

namespace duckdb {

string APIUtils::GetAwsRegion(const string host) {
	idx_t first_dot = host.find_first_of('.');
	idx_t second_dot = host.find_first_of('.', first_dot + 1);
	return host.substr(first_dot + 1, second_dot - first_dot - 1);
}

string APIUtils::GetAwsService(const string host) {
	return host.substr(0, host.find_first_of('.'));
}

string APIUtils::GetRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder,
                            const string &secret_name, const string &token, curl_slist *extra_headers) {
	if (StringUtil::StartsWith(endpoint_builder.GetHost(), "glue.") ||
	    StringUtil::StartsWith(endpoint_builder.GetHost(), "s3tables.")) {
		auto str = GetRequestAws(context, endpoint_builder, secret_name);
		return str;
	}
	auto url = endpoint_builder.GetURL();
	CURL *curl;
	CURLcode res;
	string readBuffer;

	curl = curl_easy_init();
	if (curl) {
		curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, RequestWriteCallback);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);

		if (extra_headers) {
			curl_easy_setopt(curl, CURLOPT_HTTPHEADER, extra_headers);
		}

		InitializeCurlObject(curl, token);
		res = curl_easy_perform(curl);
		curl_easy_cleanup(curl);

		DUCKDB_LOG_DEBUG(context, "iceberg.Catalog.Curl.HTTPRequest", "GET %s (curl code '%s')", url,
		                 curl_easy_strerror(res));
		if (res != CURLcode::CURLE_OK) {
			string error = curl_easy_strerror(res);
			throw IOException("Curl Request to '%s' failed with error: '%s'", url, error);
		}

		return readBuffer;
	}
	throw InternalException("Failed to initialize curl");
}

string APIUtils::GetRequestAws(ClientContext &context, IRCEndpointBuilder endpoint_builder, const string &secret_name) {
	auto clientConfig = make_uniq<Aws::Client::ClientConfiguration>();

	if (!SELECTED_CURL_CERT_PATH.empty()) {
		clientConfig->caFile = SELECTED_CURL_CERT_PATH;
	}

	std::shared_ptr<Aws::Http::HttpClientFactory> MyClientFactory;
	std::shared_ptr<Aws::Http::HttpClient> MyHttpClient;

	MyHttpClient = Aws::Http::CreateHttpClient(*clientConfig);
	Aws::Http::URI uri;

	// TODO move this to IRCatalog::GetBaseURL()
	auto service = GetAwsService(endpoint_builder.GetHost());
	auto region = GetAwsRegion(endpoint_builder.GetHost());

	for (auto &component : endpoint_builder.path_components) {
		uri.AddPathSegment(component);
	}

	for (auto &param : endpoint_builder.GetParams()) {
		const char *key = param.first.c_str();
		auto value = param.second.c_str();
		uri.AddQueryStringParameter(key, value);
	}

	Aws::Http::Scheme scheme = Aws::Http::Scheme::HTTPS;
	uri.SetScheme(scheme);
	// set host
	uri.SetAuthority(endpoint_builder.GetHost());
	auto encoded = uri.GetURLEncodedPath();

	const Aws::Http::URI uri_const = Aws::Http::URI(uri);
	auto create_http_req = Aws::Http::CreateHttpRequest(uri_const, Aws::Http::HttpMethod::HTTP_GET,
	                                                    Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);

	std::shared_ptr<Aws::Http::HttpRequest> req(create_http_req);

	// will error if no secret can be found for AWS services
	auto secret_entry = IRCatalog::GetS3Secret(context, secret_name);
	auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

	std::shared_ptr<Aws::Auth::AWSCredentialsProviderChain> provider;
	provider = std::make_shared<DuckDBSecretCredentialProvider>(
	    kv_secret.secret_map["key_id"].GetValue<string>(), kv_secret.secret_map["secret"].GetValue<string>(),
	    kv_secret.secret_map["session_token"].IsNull() ? "" : kv_secret.secret_map["session_token"].GetValue<string>());
	auto signer = make_uniq<Aws::Client::AWSAuthV4Signer>(provider, service.c_str(), region.c_str());

	signer->SignRequest(*req);
	std::shared_ptr<Aws::Http::HttpResponse> res = MyHttpClient->MakeRequest(req);
	Aws::Http::HttpResponseCode resCode = res->GetResponseCode();
	DUCKDB_LOG_DEBUG(context, "iceberg.Catalog.Aws.HTTPRequest",
	                 "GET %s (response %d) (signed with key_id '%s' for service '%s', in region '%s')",
	                 uri.GetURIString(), resCode, kv_secret.secret_map["key_id"].GetValue<string>(), service.c_str(),
	                 region.c_str());
	if (resCode == Aws::Http::HttpResponseCode::OK) {
		Aws::StringStream resBody;
		resBody << res->GetResponseBody().rdbuf();
		return resBody.str();
	} else {
		Aws::StringStream resBody;
		resBody << res->GetResponseBody().rdbuf();
		throw IOException("Failed to query %s, http error %d thrown. Message: %s", req->GetUri().GetURIString(true),
		                  res->GetResponseCode(), resBody.str());
	}
}

size_t APIUtils::RequestWriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
	((std::string *)userp)->append((char *)contents, size * nmemb);
	return size * nmemb;
}

// Look through the the above locations and if one of the files exists, set that as the location curl should use.
bool APIUtils::SelectCurlCertPath() {
	for (string &caFile : certFileLocations) {
		struct stat buf;
		if (stat(caFile.c_str(), &buf) == 0) {
			SELECTED_CURL_CERT_PATH = caFile;
		}
	}
	return false;
}

bool APIUtils::SetCurlCAFileInfo(CURL *curl) {
	if (!SELECTED_CURL_CERT_PATH.empty()) {
		curl_easy_setopt(curl, CURLOPT_CAINFO, SELECTED_CURL_CERT_PATH.c_str());
		return true;
	}
	return false;
}

// Note: every curl object we use should set this, because without it some linux distro's may not find the CA
// certificate.
void APIUtils::InitializeCurlObject(CURL *curl, const string &token) {
	if (!token.empty()) {
		curl_easy_setopt(curl, CURLOPT_XOAUTH2_BEARER, token.c_str());
		curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BEARER);
	}
	SetCurlCAFileInfo(curl);
}

string APIUtils::DeleteRequest(const string &url, const string &token, curl_slist *extra_headers) {
	CURL *curl;
	CURLcode res;
	string readBuffer;

	curl = curl_easy_init();
	if (curl) {
		curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
		curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, RequestWriteCallback);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);

		if (extra_headers) {
			curl_easy_setopt(curl, CURLOPT_HTTPHEADER, extra_headers);
		}

		InitializeCurlObject(curl, token);
		res = curl_easy_perform(curl);
		curl_easy_cleanup(curl);

		if (res != CURLcode::CURLE_OK) {
			string error = curl_easy_strerror(res);
			throw IOException("Curl DELETE Request to '%s' failed with error: '%s'", url, error);
		}

		return readBuffer;
	}
	throw InternalException("Failed to initialize curl");
}

string APIUtils::PostRequest(ClientContext &context, const string &url, const string &post_data,
                             const string &content_type, const string &token, curl_slist *extra_headers) {
	string readBuffer;
	CURL *curl = curl_easy_init();
	if (!curl) {
		throw InternalException("Failed to initialize curl");
	}

	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_POST, 1L);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_data.c_str());
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, APIUtils::RequestWriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);

	// Create default headers for content type
	struct curl_slist *headers = NULL;
	const string content_type_str = "Content-Type: application/" + content_type;
	headers = curl_slist_append(headers, content_type_str.c_str());

	// Append any extra headers
	if (extra_headers) {
		struct curl_slist *temp = extra_headers;
		while (temp) {
			headers = curl_slist_append(headers, temp->data);
			temp = temp->next;
		}
	}

	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	InitializeCurlObject(curl, token);

	// Perform the request
	CURLcode res = curl_easy_perform(curl);

	// Clean up
	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);

	DUCKDB_LOG_DEBUG(context, "iceberg.Catalog.Curl.HTTPRequest", "POST %s (curl code '%s')", url,
	                 curl_easy_strerror(res));
	if (res != CURLcode::CURLE_OK) {
		string error = curl_easy_strerror(res);
		throw IOException("Curl Request to '%s' failed with error: '%s'", url, error);
	}
	return readBuffer;
}

} // namespace duckdb
