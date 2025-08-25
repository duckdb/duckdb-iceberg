#include "iceberg_logging.hpp"
#include "mbedtls_wrapper.hpp"
#include "aws.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception/http_exception.hpp"

typedef unsigned char hash_str[64];
typedef unsigned char hash_bytes[32];

#include "duckdb/main/client_data.hpp"

#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/http/HttpClient.h>

#include <iostream>

namespace duckdb {

void sha256(const char *in, size_t in_len, hash_bytes &out) {
	duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(in, in_len, (char *)out);
}

void hmac256(const std::string &message, const char *secret, size_t secret_len, hash_bytes &out) {
	duckdb_mbedtls::MbedTlsWrapper::Hmac256(secret, secret_len, message.data(), message.size(), (char *)out);
}

void hmac256(std::string message, hash_bytes secret, hash_bytes &out) {
	hmac256(message, (char *)secret, sizeof(hash_bytes), out);
}

void hex256(hash_bytes &in, hash_str &out) {
	const char *hex = "0123456789abcdef";
	unsigned char *pin = in;
	unsigned char *pout = out;
	for (; pin < in + sizeof(in); pout += 2, pin++) {
		pout[0] = hex[(*pin >> 4) & 0xF];
		pout[1] = hex[*pin & 0xF];
	}
}

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
	std::cout << "BuildURI\n";
	Aws::Http::URI uri;
	uri.SetScheme(Aws::Http::Scheme::HTTPS);
	uri.SetAuthority(authority);
	for (auto &segment : path_segments) {
		std::cout << segment << "\n";
		uri.AddPathSegment(segment);
	}
	for (auto &param : query_string_parameters) {
		std::cout << "param\n";
		// std::cout << param.first.c_str() << " - " << param.second.c_str() << "\n";
		uri.AddQueryStringParameter(param.first.c_str(), param.second.c_str());
	}
	return uri;
}

std::shared_ptr<Aws::Http::HttpRequest> AWSInput::CreateSignedRequest(Aws::Http::HttpMethod method,
                                                                      const Aws::Http::URI &uri, const string &body,
                                                                      string content_type) {

	std::cout << "CreateHttpRequest\n";
	std::cout << uri.GetURIString() << "\n";
	std::cout << body << "\n";
	// auto request = Aws::Http::CreateHttpRequest(uri, method,Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
	// std::cout << "CreateHttpRequest done\n";
	//	request->SetUserAgent(user_agent);

	/*	if (!body.empty()) {
	{
	        auto bodyStream = Aws::MakeShared<Aws::StringStream>("");
	        *bodyStream << body;
	        request->AddContentBody(bodyStream);
	        request->SetContentLength(std::to_string(body.size()));
	        if (!content_type.empty()) {
	            request->SetHeaderValue("Content-Type", content_type);
	        }
	    }
	*/

	// std::shared_ptr<Aws::Auth::AWSCredentialsProviderChain> provider;
	// provider = std::make_shared<DuckDBSecretCredentialProvider>(key_id, secret, session_token);
	// auto signer = make_uniq<Aws::Client::AWSAuthV4Signer>(provider, service.c_str(), region.c_str());
	// if (!signer->SignRequest(*request)) {
	throw HTTPException("Failed to sign request");
	//}
	return nullptr;
	// return request;
}

unique_ptr<HTTPResponse> AWSInput::ExecuteRequest(ClientContext &context, Aws::Http::HttpMethod method,
                                                  const string body, string content_type) {

	auto clientConfig = BuildClientConfig();

	auto uri = BuildURI();
	auto &db = DatabaseInstance::GetDatabase(context);

	HTTPHeaders res(db);
	//       headers.Insert("X-Iceberg-Access-Delegation", "vended-credentials");
	//       if (!token.empty()) {
	//               headers.Insert("Authorization", StringUtil::Format("Bearer %s", token));
	//       }
	{
		res["Host"] = uri.GetURIString();
		// If access key is not set, we don't set the headers at all to allow accessing public files through s3 urls

		string payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"; // Empty payload hash
		// key_id, secret, session_token
		// we can pass date/time but this is mostly useful in testing. normally we just get the current datetime here.
		auto timestamp = Timestamp::GetCurrentTimestamp();
		string date_now = StrfTimeFormat::Format(timestamp, "%Y%m%d");
		string datetime_now = StrfTimeFormat::Format(timestamp, "%Y%m%dT%H%M%SZ");

		res["x-amz-date"] = datetime_now;
		res["x-amz-content-sha256"] = payload_hash;
		if (session_token.length() > 0) {
			res["x-amz-security-token"] = session_token;
		}

		string signed_headers = "";
		hash_bytes canonical_request_hash;
		hash_str canonical_request_hash_str;
		if (content_type.length() > 0) {
			signed_headers += "content-type;";
		}
		signed_headers += "host;x-amz-content-sha256;x-amz-date";
		if (session_token.length() > 0) {
			signed_headers += ";x-amz-security-token";
		}

		string XX = uri.GetURIString().substr(8 + 32);
		if (uri.GetQueryString().size() > 0) {
			XX = XX.substr(0, XX.size() - uri.GetQueryString().size());
		}
		{
			string YY = "";
			for (auto c : XX) {
				if (c == 'F')
					YY += "52F";
				else if (c != ':')
					YY += c;
				else
					YY += "%3A";
			}
			std::cout << XX << "\t" << YY << "\n";
			XX = YY;
		}

		auto canonical_request = string(Aws::Http::HttpMethodMapper::GetNameForHttpMethod(method)) + "\n" + XX + "\n";
		if (uri.GetQueryString().size()) {
			canonical_request += uri.GetQueryString().substr(1);
		}

		// if (content_type.length() > 0) {
		//		canonical_request += "\ncontent-type:" + content_type;
		//	}
		string host = "s3tables.us-east-2.amazonaws.com";
		canonical_request +=
		    "\nhost:" + host + "\nx-amz-content-sha256:" + payload_hash + "\nx-amz-date:" + datetime_now;
		if (session_token.length() > 0) {
			canonical_request += "\nx-amz-security-token:" + session_token;
		}
		//	if (use_sse_kms) {
		//		canonical_request += "\nx-amz-server-side-encryption:aws:kms";
		//		canonical_request += "\nx-amz-server-side-encryption-aws-kms-key-id:" + arams.kms_key_id;
		//	}
		//	if (use_requester_pays) {
		//		canonical_request += "\nx-amz-request-payer:requester";
		//	}

		canonical_request += "\n\n" + signed_headers + "\n" + payload_hash;
		std::cout << canonical_request << "\nCANONICAL_REQUEST_END\n";
		sha256(canonical_request.c_str(), canonical_request.length(), canonical_request_hash);

		hex256(canonical_request_hash, canonical_request_hash_str);
		auto string_to_sign = "AWS4-HMAC-SHA256\n" + datetime_now + "\n" + date_now + "/" + "us-east-2" + "/" +
		                      service + "/aws4_request\n" +
		                      string((char *)canonical_request_hash_str, sizeof(hash_str));

		std::cout << string_to_sign << "\nSTRING_TO_SIGN_END\n";
		// compute signature
		hash_bytes k_date, k_region, k_service, signing_key, signature;
		hash_str signature_str;
		auto sign_key = "AWS4" + secret;
		hmac256(date_now, sign_key.c_str(), sign_key.length(), k_date);
		hmac256("us-east-2", k_date, k_region);
		hmac256(service, k_region, k_service);
		hmac256("aws4_request", k_service, signing_key);
		hmac256(string_to_sign, signing_key, signature);
		hex256(signature, signature_str);

		res["Authorization"] = "AWS4-HMAC-SHA256 Credential=" + key_id + "/" + date_now + "/" + "us-east-2" + "/" +
		                       service + "/aws4_request, SignedHeaders=" + signed_headers +
		                       ", Signature=" + string((char *)signature_str, sizeof(hash_str));
	}

	auto &http_util = HTTPUtil::Get(db);
	unique_ptr<HTTPParams> params;

	string request_url = "https://cors-proxy.carlo-f90.workers.dev/corsproxy/" + uri.GetURIString().substr(8);

	params = http_util.InitializeParameters(context, request_url);

	if (!body.empty()) {
		std::cout << "body not empty\n";
		/*
		                auto bodyStream = Aws::MakeShared<Aws::StringStream>("");
		                *bodyStream << body;
		                request->AddContentBody(bodyStream);
		                request->SetContentLength(std::to_string(body.size()));
		                if (!content_type.empty()) {
		                        request->SetHeaderValue("Content-Type", content_type);
		                }
		*/
	}

	GetRequestInfo get_request(request_url, res, *params, nullptr, nullptr);
	return http_util.Request(get_request);

	std::cout << "ExecuteRequest\n";
	//	InitAWSAPI();
	std::cout << "BuildURI\n";
	std::cout << "CreateSignedRequest\n";
	auto request = CreateSignedRequest(method, uri, body, content_type);
	std::cout << "CreateSignedRequest done\n";

	LogAWSRequest(context, request);
	auto result = make_uniq<HTTPResponse>(HTTPStatusCode::INVALID);

	return result;
	/*
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
	        result->success = false;
	    } else {
	        Aws::StringStream resBody;
	        resBody << response->GetResponseBody().rdbuf();
	        result->body = resBody.str();
	    }
	    return result;
	*/
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

} // namespace duckdb
