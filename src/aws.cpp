#include "iceberg_logging.hpp"

#include "aws.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception/http_exception.hpp"

#ifdef EMSCRIPTEN
#else
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/http/HttpClient.h>
#endif

namespace duckdb {

#ifdef EMSCRIPTEN

unique_ptr<HTTPResponse> AWSInput::GetRequest(ClientContext &context) {
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

Aws::Client::ClientConfiguration AWSInput::BuildClientConfig() {
	auto config = Aws::Client::ClientConfiguration();
	if (!cert_path.empty()) {
		config.caFile = cert_path;
	}
	return config;
}

Aws::Http::URI AWSInput::BuildURI() {
	Aws::Http::URI uri;
	uri.SetScheme(Aws::Http::Scheme::HTTPS);
	uri.SetAuthority(authority);
	for (auto &segment : path_segments) {
		uri.AddPathSegment(segment);
	}
	for (auto &param : query_string_parameters) {
		uri.AddQueryStringParameter(param.first.c_str(), param.second.c_str());
	}
	return uri;
}

std::shared_ptr<Aws::Http::HttpRequest> AWSInput::CreateSignedRequest(Aws::Http::HttpMethod method,
                                                                      const Aws::Http::URI &uri, const string &body,
                                                                      string content_type) {

	auto request = Aws::Http::CreateHttpRequest(uri, method, Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
	request->SetUserAgent(user_agent);

	if (!body.empty()) {
		auto bodyStream = Aws::MakeShared<Aws::StringStream>("");
		*bodyStream << body;
		request->AddContentBody(bodyStream);
		request->SetContentLength(std::to_string(body.size()));
		if (!content_type.empty()) {
			request->SetHeaderValue("Content-Type", content_type);
		}
	}

	std::shared_ptr<Aws::Auth::AWSCredentialsProviderChain> provider;
	provider = std::make_shared<DuckDBSecretCredentialProvider>(key_id, secret, session_token);
	auto signer = make_uniq<Aws::Client::AWSAuthV4Signer>(provider, service.c_str(), region.c_str());
	if (!signer->SignRequest(*request)) {
		throw HTTPException("Failed to sign request");
	}

	return request;
}

unique_ptr<HTTPResponse> AWSInput::ExecuteRequest(ClientContext &context, Aws::Http::HttpMethod method,
                                                  const string body, string content_type) {

	InitAWSAPI();
	auto clientConfig = BuildClientConfig();
	auto uri = BuildURI();
	auto request = CreateSignedRequest(method, uri, body, content_type);

	LogAWSRequest(context, request);
	auto httpClient = Aws::Http::CreateHttpClient(clientConfig);
	auto response = httpClient->MakeRequest(request);
	auto resCode = response->GetResponseCode();

	DUCKDB_LOG(context, IcebergLogType,
	           "%s %s (response %d) (signed with key_id '%s' for service '%s', in region '%s')",
	           Aws::Http::HttpMethodMapper::GetNameForHttpMethod(method), uri.GetURIString(), resCode, key_id,
	           service.c_str(), region.c_str());

	auto result = make_uniq<HTTPResponse>(resCode == Aws::Http::HttpResponseCode::REQUEST_NOT_MADE
	                                          ? HTTPStatusCode::INVALID
	                                          : HTTPStatusCode(static_cast<idx_t>(resCode)));

	result->url = uri.GetURIString();
	if (resCode == Aws::Http::HttpResponseCode::REQUEST_NOT_MADE) {
		D_ASSERT(response->HasClientError());
		result->reason = response->GetClientErrorMessage();
	} else {
		Aws::StringStream resBody;
		resBody << response->GetResponseBody().rdbuf();
		result->body = resBody.str();
	}
	return result;
}

unique_ptr<HTTPResponse> AWSInput::GetRequest(ClientContext &context) {
	return ExecuteRequest(context, Aws::Http::HttpMethod::HTTP_GET);
}

unique_ptr<HTTPResponse> AWSInput::DeleteRequest(ClientContext &context) {
	return ExecuteRequest(context, Aws::Http::HttpMethod::HTTP_DELETE);
}

unique_ptr<HTTPResponse> AWSInput::PostRequest(ClientContext &context, string post_body) {
	return ExecuteRequest(context, Aws::Http::HttpMethod::HTTP_POST, post_body, "application/json");
}

#endif

} // namespace duckdb
