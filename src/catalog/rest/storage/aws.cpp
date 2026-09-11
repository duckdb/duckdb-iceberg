#include "catalog/rest/storage/aws.hpp"

#include "duckdb/common/http_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/main/client_data.hpp"
#include "mbedtls_wrapper.hpp"

#include "iceberg_logging.hpp"
#include "catalog/rest/storage/iceberg_authorization.hpp"

namespace duckdb {

namespace {

typedef unsigned char hash_str[64];
typedef unsigned char hash_bytes[32];

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

//! The verb as it appears on the first line of the SigV4 canonical request.
const char *MethodName(RequestType request_type) {
	switch (request_type) {
	case RequestType::GET_REQUEST:
		return "GET";
	case RequestType::PUT_REQUEST:
		return "PUT";
	case RequestType::HEAD_REQUEST:
		return "HEAD";
	case RequestType::DELETE_REQUEST:
		return "DELETE";
	case RequestType::POST_REQUEST:
		return "POST";
	default:
		throw NotImplementedException("Cannot sign a request of type %s", EnumUtil::ToString(request_type));
	}
}

//! Aws::Http::URI::AddPathSegment stripped leading and trailing slashes from every segment it
//! was given, and kept interior ones (which is why the canonical path needs the %2F rewrite
//! below). Segments are stored raw here, so do it on the way out.
string NormalizeSegment(const string &segment) {
	auto begin = segment.find_first_not_of('/');
	if (begin == string::npos) {
		return "";
	}
	auto end = segment.find_last_not_of('/');
	return segment.substr(begin, end - begin + 1);
}

//! The SDK's non-RFC path encoder (urlEncodeSegment with s_compliantRfc3986Encoding false).
//! Unreserved characters plus the reserved set AWS chose to leave alone for compatibility.
string WireEncodeSegment(const string &segment) {
	static const char *HEX_DIGIT = "0123456789ABCDEF";
	string result;
	for (auto character : segment) {
		auto ch = static_cast<unsigned char>(character);
		if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9')) {
			result += character;
			continue;
		}
		switch (ch) {
		// RFC 3986 unreserved
		case '-':
		case '_':
		case '.':
		case '~':
		// Reserved, but deliberately not escaped by the SDK, to match services that never
		// escaped them either.
		case '$':
		case '&':
		case ',':
		case ':':
		case '=':
		case '@':
			result += character;
			break;
		default:
			result += '%';
			result += HEX_DIGIT[ch >> 4];
			result += HEX_DIGIT[ch & 15];
		}
	}
	return result;
}

string GetPayloadHash(const char *buffer, idx_t buffer_len) {
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

} // namespace

string AWSInput::CanonicalPath() const {
	if (path_segments.empty()) {
		return "/";
	}
	string result;
	for (auto &segment : path_segments) {
		result += "/" + StringUtil::URLEncode(NormalizeSegment(segment));
	}
	return result;
}

string AWSInput::WirePath() const {
	// GetURIString appended no path at all when the segment list was empty, rather than "/".
	string result;
	for (auto &segment : path_segments) {
		result += "/" + WireEncodeSegment(NormalizeSegment(segment));
	}
	return result;
}

string AWSInput::QueryString() const {
	string result;
	for (auto &param : query_string_parameters) {
		result += result.empty() ? "?" : "&";
		result += StringUtil::URLEncode(param.first) + "=" + StringUtil::URLEncode(param.second);
	}
	return result;
}

string AWSInput::URL() const {
	return string(use_https ? "https://" : "http://") + authority + WirePath() + QueryString();
}

unique_ptr<HTTPResponse> AWSInput::Request(RequestType request_type, ClientContext &context, HTTPHeaders &headers,
                                           const string &data) {
	auto &db = DatabaseInstance::GetDatabase(context);

	HTTPHeaders res(db);

	res["host"] = authority;
	// If access key is not set, we don't set the headers at all to allow accessing public files through s3 urls

	string payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"; // Empty payload hash

	if (!data.empty()) {
		payload_hash = GetPayloadHash(data.c_str(), data.size());
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

	string url_encoded_path = CanonicalPath();

	{
		// it's unclear to be why we need to transform %2F into %252F, see
		// https://en.wikipedia.org/wiki/Percent-encoding#Percent_character
		url_encoded_path = StringUtil::Replace(url_encoded_path, "%2F", "%252F");
	}

	auto query_string = QueryString();

	auto canonical_request = string(MethodName(request_type)) + "\n" + url_encoded_path + "\n";
	if (query_string.size()) {
		canonical_request += query_string.substr(1);
	}

	if (content_type.length() > 0) {
		canonical_request += "\ncontent-type:" + content_type;
	}
	canonical_request +=
	    "\nhost:" + authority + "\nx-amz-content-sha256:" + payload_hash + "\nx-amz-date:" + datetime_now;
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

	string request_url = URL();

	params = http_util.InitializeParameters(context, request_url);

	auto locked_client = IcebergAuthorizationContextState::GetHTTPClient(attached_db, context);
	auto &client = locked_client.GetClient();
	if (client) {
		client->Initialize(*params);
	}

	switch (request_type) {
	case RequestType::HEAD_REQUEST: {
		HeadRequestInfo head_request(request_url, res, *params);
		return http_util.Request(head_request, client);
	}
	case RequestType::DELETE_REQUEST: {
		DeleteRequestInfo delete_request(request_url, res, *params);
		return http_util.Request(delete_request, client);
	}
	case RequestType::GET_REQUEST: {
		GetRequestInfo get_request(request_url, res, *params, nullptr, nullptr);
		return http_util.Request(get_request, client);
	}
	case RequestType::POST_REQUEST: {
		PostRequestInfo post_request(request_url, res, *params, reinterpret_cast<const_data_ptr_t>(data.c_str()),
		                             data.size());
		auto x = http_util.Request(post_request, client);
		if (x) {
			x->body = post_request.buffer_out;
		}
		return x;
	}
	default:
		throw NotImplementedException("Cannot make request of type %s", EnumUtil::ToString(request_type));
	}
}

} // namespace duckdb
