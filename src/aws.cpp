#include "iceberg_logging.hpp"

#include "aws.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "include/storage/irc_authorization.hpp"

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

static void LogAWSRequest(ClientContext &context, std::shared_ptr<Aws::Http::HttpRequest> &req,
                          Aws::Http::HttpMethod &method) {
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
		RequestType type;
		switch (method) {
		case Aws::Http::HttpMethod::HTTP_GET:
			type = RequestType::GET_REQUEST;
			break;
		case Aws::Http::HttpMethod::HTTP_HEAD:
			type = RequestType::HEAD_REQUEST;
			break;
		case Aws::Http::HttpMethod::HTTP_DELETE:
			type = RequestType::DELETE_REQUEST;
			break;
		case Aws::Http::HttpMethod::HTTP_POST:
			type = RequestType::POST_REQUEST;
			break;
		case Aws::Http::HttpMethod::HTTP_PUT:
			type = RequestType::PUT_REQUEST;
			break;
		default:
			throw InvalidConfigurationException("Aws client cannot create request of type %s",
			                                    Aws::Http::HttpMethodMapper::GetNameForHttpMethod(method));
		}
		auto request = BaseRequest(type, url, http_headers, params);
		request.params.logger = context.logger;
		http_util.LogRequest(request, nullptr);
	}
}

Aws::Client::ClientConfiguration AWSInput::BuildClientConfig() {
	auto config = Aws::Client::ClientConfiguration();
	if (!cert_path.empty()) {
		config.caFile = cert_path;
	}
	if (use_httpfs_timeout) {
		// requestTimeoutMS is for Windows
		config.requestTimeoutMs = request_timeout_in_ms;
		// httpRequestTimoutMS is for all other OS's
		// see
		// https://github.com/aws/aws-sdk-cpp/blob/199c0a80b29a30db35b8d23c043aacf7ccb28957/src/aws-cpp-sdk-core/include/aws/core/client/ClientConfiguration.h#L190
		config.httpRequestTimeoutMs = request_timeout_in_ms;
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
                                                                      const Aws::Http::URI &uri, HTTPHeaders &headers,
                                                                      const string &body) {

	auto request = Aws::Http::CreateHttpRequest(uri, method, Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
	request->SetUserAgent(user_agent);

	if (!body.empty()) {
		auto bodyStream = Aws::MakeShared<Aws::StringStream>("");
		*bodyStream << body;
		request->AddContentBody(bodyStream);
		request->SetContentLength(std::to_string(body.size()));
		if (headers.HasHeader("Content-Type")) {
			request->SetHeaderValue("Content-Type", headers.GetHeaderValue("Content-Type"));
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
                                                  HTTPHeaders &headers, const string &body) {

	InitAWSAPI();
	auto clientConfig = BuildClientConfig();
	auto uri = BuildURI();
	auto request = CreateSignedRequest(method, uri, headers, body);

	LogAWSRequest(context, request, method);
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
		throw HTTPException(*result, result->reason);
	}
	Aws::StringStream resBody;
	resBody << response->GetResponseBody().rdbuf();
	result->body = resBody.str();

	if (static_cast<uint16_t>(result->status) > 400) {
		result->success = false;
	}
	return result;
}

unique_ptr<HTTPResponse> AWSInput::Request(RequestType request_type, ClientContext &context, HTTPHeaders &headers,
                                           const string &data) {
	switch (request_type) {
	case RequestType::GET_REQUEST:
		return ExecuteRequest(context, Aws::Http::HttpMethod::HTTP_GET, headers);
	case RequestType::POST_REQUEST:
		return ExecuteRequest(context, Aws::Http::HttpMethod::HTTP_POST, headers, data);
	case RequestType::DELETE_REQUEST:
		return ExecuteRequest(context, Aws::Http::HttpMethod::HTTP_DELETE, headers);
	case RequestType::HEAD_REQUEST:
		return ExecuteRequest(context, Aws::Http::HttpMethod::HTTP_HEAD, headers);
	default:
		throw NotImplementedException("Cannot make request of type %s", EnumUtil::ToString(request_type));
	}
}

#endif

} // namespace duckdb
