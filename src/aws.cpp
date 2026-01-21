#include "iceberg_logging.hpp"
#include "mbedtls_wrapper.hpp"
#include "aws.hpp"
#include "hash_utils.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/main/client_data.hpp"
#include "include/storage/irc_authorization.hpp"

#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/http/HttpClient.h>

namespace duckdb {

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

static void LogAWSHTTPRequest(ClientContext &context, std::shared_ptr<Aws::Http::HttpRequest> &req,
                              HTTPResponse &response, Aws::Http::HttpMethod &method) {
	D_ASSERT(context.db);
	auto http_util = HTTPUtil::Get(*context.db);
	auto aws_headers = req->GetHeaders();
	auto http_headers = HTTPHeaders();
	for (auto &header : aws_headers) {
		http_headers.Insert(header.first, header.second);
	}
	auto params = HTTPParams(http_util);
	auto url = string("https://") + req->GetUri().GetAuthority() + req->GetUri().GetPath();
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
	auto request = BaseRequest(type, url, std::move(http_headers), params);
	request.params.logger = context.logger;
	http_util.LogRequest(request, response);
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

static string GetPayloadHash(const char *buffer, idx_t buffer_len) {
	if (buffer_len > 0) {
		hash_bytes payload_hash_bytes;
		hash_str payload_hash_str;
		sha256(buffer, buffer_len, payload_hash_bytes);
		hex256(payload_hash_bytes, payload_hash_str);
		return string((char *)payload_hash_str, sizeof(payload_hash_str));
	} else {
		return "";
	}
}

std::shared_ptr<Aws::Http::HttpRequest> AWSInput::CreateSignedRequest(Aws::Http::HttpMethod method,
                                                                      const Aws::Http::URI &uri, HTTPHeaders &headers,
                                                                      const string &body) {
#ifndef EMSCRIPTEN
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
#else
	return nullptr;
#endif
}

unique_ptr<HTTPResponse> AWSInput::ExecuteRequestLegacy(ClientContext &context, Aws::Http::HttpMethod method,
                                                        HTTPHeaders &headers, const string &body) {
#ifndef EMSCRIPTEN
	InitAWSAPI();
	auto clientConfig = BuildClientConfig();
	auto uri = BuildURI();
	auto request = CreateSignedRequest(method, uri, headers, body);

	auto httpClient = Aws::Http::CreateHttpClient(clientConfig);
	auto response = httpClient->MakeRequest(request);
	auto resCode = response->GetResponseCode();

	auto result = make_uniq<HTTPResponse>(resCode == Aws::Http::HttpResponseCode::REQUEST_NOT_MADE
	                                          ? HTTPStatusCode::INVALID
	                                          : HTTPStatusCode(static_cast<idx_t>(resCode)));

	bool throw_exception = false;
	result->url = uri.GetURIString();
	if (resCode == Aws::Http::HttpResponseCode::REQUEST_NOT_MADE) {
		D_ASSERT(response->HasClientError());
		result->reason = response->GetClientErrorMessage();
		throw_exception = true;
	}
	for (auto &header : response->GetHeaders()) {
		result->headers[header.first] = header.second;
	}
	Aws::StringStream resBody;
	resBody << response->GetResponseBody().rdbuf();
	result->body = resBody.str();
	if (static_cast<uint16_t>(result->status) > 400) {
		result->success = false;
	}
	LogAWSHTTPRequest(context, request, *result, method);
	if (throw_exception) {
		throw HTTPException(*result, result->reason);
	}
	return result;
#else
	throw NotImplementedException("ExecuteRequestLegacy is not implemented in duckdb-wasm");
#endif
}

unique_ptr<HTTPResponse> AWSInput::ExecuteRequest(ClientContext &context, Aws::Http::HttpMethod method,
                                                  unique_ptr<HTTPClient> &client, HTTPHeaders &headers,
                                                  const string &body) {
	bool use_httputils = true;
	{
		Value result;
		(void)context.TryGetCurrentSetting("iceberg_via_aws_sdk_for_catalog_interactions", result);
		if (!result.IsNull() && result.GetValue<bool>()) {
			use_httputils = false;
		}
	}
	if (!use_httputils) {
		// Query Iceberg REST catalog via AWS's SDK
		return ExecuteRequestLegacy(context, method, headers, body);
	}

	auto uri = BuildURI();
	auto &db = DatabaseInstance::GetDatabase(context);

	HTTPHeaders res(db);

	const string host = uri.GetAuthority();
	res["host"] = host;
	// If access key is not set, we don't set the headers at all to allow accessing public files through s3 urls

	string payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"; // Empty payload hash

	if (!body.empty()) {
		payload_hash = GetPayloadHash(body.c_str(), body.size());
	}

	// key_id, secret, session_token
	// we can pass date/time but this is mostly useful in testing. normally we just get the current datetime
	// here.
	auto timestamp = Timestamp::GetCurrentTimestamp();
	string date_now = StrfTimeFormat::Format(timestamp, "%Y%m%d");
	string datetime_now = StrfTimeFormat::Format(timestamp, "%Y%m%dT%H%M%SZ");

	res["x-amz-date"] = datetime_now;
	res["x-amz-content-sha256"] = payload_hash;
	if (session_token.length() > 0) {
		res["x-amz-security-token"] = session_token;
	}
	string content_type;
	if (headers.HasHeader("Content-Type")) {
		content_type = headers.GetHeaderValue("Content-Type");
	}
	if (!content_type.empty()) {
		res["Content-Type"] = content_type;
	}
	string signed_headers = "";
	hash_bytes canonical_request_hash;
	hash_str canonical_request_hash_str;
	if (content_type.length() > 0) {
		signed_headers += "content-type;";
		res["Content-Type"] = content_type;
	}
	signed_headers += "host;x-amz-content-sha256;x-amz-date";
	if (session_token.length() > 0) {
		signed_headers += ";x-amz-security-token";
	}

	string url_encoded_path = uri.GetURLEncodedPath();

	{
		// it's unclear to be why we need to transform %2F into %252F, see
		// https://en.wikipedia.org/wiki/Percent-encoding#Percent_character
		url_encoded_path = StringUtil::Replace(url_encoded_path, "%2F", "%252F");
	}

	auto canonical_request =
	    string(Aws::Http::HttpMethodMapper::GetNameForHttpMethod(method)) + "\n" + url_encoded_path + "\n";
	if (uri.GetQueryString().size()) {
		canonical_request += uri.GetQueryString().substr(1);
	}

	if (content_type.length() > 0) {
		canonical_request += "\ncontent-type:" + content_type;
	}
	canonical_request += "\nhost:" + host + "\nx-amz-content-sha256:" + payload_hash + "\nx-amz-date:" + datetime_now;
	if (session_token.length() > 0) {
		canonical_request += "\nx-amz-security-token:" + session_token;
	}
	canonical_request += "\n\n" + signed_headers + "\n" + payload_hash;
	sha256(canonical_request.c_str(), canonical_request.length(), canonical_request_hash);

	hex256(canonical_request_hash, canonical_request_hash_str);
	auto string_to_sign = "AWS4-HMAC-SHA256\n" + datetime_now + "\n" + date_now + "/" + region + "/" + service +
	                      "/aws4_request\n" + string((char *)canonical_request_hash_str, sizeof(hash_str));

	// TODO: DUCKDB_LOGS (canonical_request + string_to_sing)

	// compute signature
	hash_bytes k_date, k_region, k_service, signing_key, signature;
	hash_str signature_str;
	auto sign_key = "AWS4" + secret;
	hmac256(date_now, sign_key.c_str(), sign_key.length(), k_date);
	hmac256(region, k_date, k_region);
	hmac256(service, k_region, k_service);
	hmac256("aws4_request", k_service, signing_key);
	hmac256(string_to_sign, signing_key, signature);
	hex256(signature, signature_str);

	res["Authorization"] = "AWS4-HMAC-SHA256 Credential=" + key_id + "/" + date_now + "/" + region + "/" + service +
	                       "/aws4_request, SignedHeaders=" + signed_headers +
	                       ", Signature=" + string((char *)signature_str, sizeof(hash_str));

	auto &http_util = HTTPUtil::Get(db);
	unique_ptr<HTTPParams> params;

	string request_url = uri.GetURIString();

	params = http_util.InitializeParameters(context, request_url);

	if (client) {
		client->Initialize(*params);
	}

	switch (method) {
	case Aws::Http::HttpMethod::HTTP_HEAD: {
		HeadRequestInfo head_request(request_url, res, *params);
		return http_util.Request(head_request, client);
	}
	case Aws::Http::HttpMethod::HTTP_DELETE: {
		DeleteRequestInfo delete_request(request_url, res, *params);
		return http_util.Request(delete_request, client);
	}
	case Aws::Http::HttpMethod::HTTP_GET: {
		GetRequestInfo get_request(request_url, res, *params, nullptr, nullptr);
		return http_util.Request(get_request, client);
	}
	case Aws::Http::HttpMethod::HTTP_POST: {
		PostRequestInfo post_request(request_url, res, *params, reinterpret_cast<const_data_ptr_t>(body.c_str()),
		                             body.size());
		auto x = http_util.Request(post_request, client);
		if (x) {
			x->body = post_request.buffer_out;
		}
		return std::move(x);
	}
	default:
		throw NotImplementedException("Unexpected HTTP Method requested");
	}
}

unique_ptr<HTTPResponse> AWSInput::Request(RequestType request_type, ClientContext &context,
                                           unique_ptr<HTTPClient> &client, HTTPHeaders &headers, const string &data) {
	switch (request_type) {
	case RequestType::GET_REQUEST:
		return ExecuteRequest(context, Aws::Http::HttpMethod::HTTP_GET, client, headers);
	case RequestType::POST_REQUEST:
		return ExecuteRequest(context, Aws::Http::HttpMethod::HTTP_POST, client, headers, data);
	case RequestType::DELETE_REQUEST:
		return ExecuteRequest(context, Aws::Http::HttpMethod::HTTP_DELETE, client, headers);
	case RequestType::HEAD_REQUEST:
		return ExecuteRequest(context, Aws::Http::HttpMethod::HTTP_HEAD, client, headers);
	default:
		throw NotImplementedException("Cannot make request of type %s", EnumUtil::ToString(request_type));
	}
}

} // namespace duckdb
