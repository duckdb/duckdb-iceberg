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

#include <iostream>

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

#ifndef EMSCRIPTEN
	// auto request = Aws::Http::CreateHttpRequest(uri, method,Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
	//	request->SetUserAgent(user_agent);

	if (!body.empty()) {
		auto bodyStream = Aws::MakeShared<Aws::StringStream>("");
		*bodyStream << body;
		request->AddContentBody(bodyStream);
		request->SetContentLength(std::to_string(body.size()));
		if (headers.HasHeader("Content-Type")) {
			request->SetHeaderValue("Content-Type", headers.GetHeaderValue("Content-Type"));
		}
	}

	// std::shared_ptr<Aws::Auth::AWSCredentialsProviderChain> provider;
	// provider = std::make_shared<DuckDBSecretCredentialProvider>(key_id, secret, session_token);
	// auto signer = make_uniq<Aws::Client::AWSAuthV4Signer>(provider, service.c_str(), region.c_str());
	// if (!signer->SignRequest(*request)) {
	throw HTTPException("Failed to sign request");
	//}
#endif
	return nullptr;
	// return request;
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

unique_ptr<HTTPResponse> AWSInput::ExecuteRequest(ClientContext &context, Aws::Http::HttpMethod method,
                                                  HTTPHeaders &headers, const string &body) {

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
#ifdef EMSCRIPTEN
			res["content-type"] = content_type;
#endif
		}
		signed_headers += "host;x-amz-content-sha256;x-amz-date";
		if (session_token.length() > 0) {
			signed_headers += ";x-amz-security-token";
		}

		string XX = uri.GetURLEncodedPath();

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
			XX = YY;
		}

		auto canonical_request = string(Aws::Http::HttpMethodMapper::GetNameForHttpMethod(method)) + "\n" + XX + "\n";
		if (uri.GetQueryString().size()) {
			canonical_request += uri.GetQueryString().substr(1);
		}

		if (content_type.length() > 0) {
			canonical_request += "\ncontent-type:" + content_type;
		}
		string host = uri.GetAuthority();
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
		sha256(canonical_request.c_str(), canonical_request.length(), canonical_request_hash);

		hex256(canonical_request_hash, canonical_request_hash_str);
		auto string_to_sign = "AWS4-HMAC-SHA256\n" + datetime_now + "\n" + date_now + "/" + region + "/" + service +
		                      "/aws4_request\n" + string((char *)canonical_request_hash_str, sizeof(hash_str));

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
	}

	auto &http_util = HTTPUtil::Get(db);
	unique_ptr<HTTPParams> params;

	string request_url = uri.GetURIString().substr(8);

	Value val;
	auto has_setting = context.TryGetCurrentSetting("experimental_s3_tables_global_proxy", val);
	if (has_setting) {
		request_url = val.GetValue<string>() + request_url;
	} else {
		request_url = "https://" + request_url;
	}

	params = http_util.InitializeParameters(context, request_url);

	if (method == Aws::Http::HttpMethod::HTTP_HEAD) {
		HeadRequestInfo head_request(request_url, res, *params);
		return http_util.Request(head_request);
	}
	if (method == Aws::Http::HttpMethod::HTTP_DELETE) {
		DeleteRequestInfo delete_request(request_url, res, *params);
		return http_util.Request(delete_request);
	}
	if (method == Aws::Http::HttpMethod::HTTP_GET) {
		GetRequestInfo get_request(request_url, res, *params, nullptr, nullptr);
		return http_util.Request(get_request);
	}
	if (method == Aws::Http::HttpMethod::HTTP_POST) {
		PostRequestInfo post_request(request_url, res, *params, reinterpret_cast<const_data_ptr_t>(body.c_str()),
		                             body.size());
		return http_util.Request(post_request);
	}
	throw NotImplementedException("Only GET and POST are implemented at the moment");
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

} // namespace duckdb
