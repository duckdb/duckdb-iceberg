#include "iceberg_logging.hpp"

#include "aws.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception/http_exception.hpp"

#ifdef EMSCRIPTEN
#else
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpRequest.h>
#endif

namespace duckdb {

#ifdef EMSCRIPTEN

string AWSInput::GetRequest(ClientContext &context) {
	throw NotImplementedException("GET on WASM not implemented yet");
}

#else

namespace {

class DuckDBSecretCredentialProvider : public Aws::Auth::AWSCredentialsProviderChain {
public:
	DuckDBSecretCredentialProvider(const string &key_id, const string &secret, const string &sesh_token) {
		credentials.SetAWSAccessKeyId(key_id);
		credentials.SetAWSSecretKey(secret);
		credentials.SetSessionToken(sesh_token);
	}

	~DuckDBSecretCredentialProvider() = default;

	Aws::Auth::AWSCredentials GetAWSCredentials() override {
		return credentials;
	};

protected:
	Aws::Auth::AWSCredentials credentials;
};

} // namespace

static void InitAWSAPI() {
	static bool loaded = false;
	if (!loaded) {
		Aws::SDKOptions options;
		Aws::InitAPI(options); // Should only be called once.
		loaded = true;
	}
}

static void LogAWSRequest(ClientContext &context, std::shared_ptr<Aws::Http::HttpRequest> &req) {
	if (context.db) {
		auto http_util = HTTPUtil::Get(*context.db);
		auto aws_headers = req->GetHeaders();
		auto http_headers = HTTPHeaders();
		for (auto &header : aws_headers) {
			http_headers.Insert(header.first.c_str(), header.second);
		}
		auto params = HTTPParams(http_util);
		auto url = "https://" + req->GetUri().GetAuthority() + req->GetUri().GetPath();
		const auto query_str = req->GetUri().GetQueryString();
		if (!query_str.empty()) {
			url += "?" + query_str;
		}
		auto request = GetRequestInfo(
		    url, http_headers, params, [](const HTTPResponse &response) { return false; },
		    [](const_data_ptr_t data, idx_t data_length) { return false; });
		request.params.logger = context.logger;
		http_util.LogRequest(request, nullptr);
	}
}

string AWSInput::GetRequest(ClientContext &context) {
	InitAWSAPI();
	auto clientConfig = make_uniq<Aws::Client::ClientConfiguration>();

	if (!cert_path.empty()) {
		clientConfig->caFile = cert_path;
	}

	Aws::Http::URI uri;
	Aws::Http::Scheme scheme = Aws::Http::Scheme::HTTPS;
	uri.SetScheme(scheme);
	uri.SetAuthority(authority);
	for (auto &segment : path_segments) {
		uri.AddPathSegment(segment);
	}

	for (auto &param : query_string_parameters) {
		uri.AddQueryStringParameter(param.first.c_str(), param.second.c_str());
	}

	std::shared_ptr<Aws::Auth::AWSCredentialsProviderChain> provider;
	provider = std::make_shared<DuckDBSecretCredentialProvider>(key_id, secret, session_token);
	auto signer = make_uniq<Aws::Client::AWSAuthV4Signer>(provider, service.c_str(), region.c_str());

	const Aws::Http::URI uri_const = Aws::Http::URI(uri);
	auto create_http_req = Aws::Http::CreateHttpRequest(uri_const, Aws::Http::HttpMethod::HTTP_GET,
	                                                    Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
	std::shared_ptr<Aws::Http::HttpRequest> req(create_http_req);
	req->SetUserAgent(user_agent);

	signer->SignRequest(*req);

	std::shared_ptr<Aws::Http::HttpClient> MyHttpClient;
	MyHttpClient = Aws::Http::CreateHttpClient(*clientConfig);

	LogAWSRequest(context, req);
	std::shared_ptr<Aws::Http::HttpResponse> res = MyHttpClient->MakeRequest(req);
	Aws::Http::HttpResponseCode resCode = res->GetResponseCode();
	DUCKDB_LOG(context, IcebergLogType,
	           "GET %s (response %d) (signed with key_id '%s' for service '%s', in region '%s')", uri.GetURIString(),
	           resCode, key_id, service.c_str(), region.c_str());

	if (resCode != Aws::Http::HttpResponseCode::OK) {
		Aws::StringStream resBody;
		resBody << res->GetResponseBody().rdbuf();
		throw HTTPException(StringUtil::Format("Failed to query %s, http error %d thrown. Message: %s",
		                                       req->GetUri().GetURIString(true), res->GetResponseCode(),
		                                       resBody.str()));
	}
	Aws::StringStream resBody;
	resBody << res->GetResponseBody().rdbuf();
	return resBody.str();
}

#endif

} // namespace duckdb
