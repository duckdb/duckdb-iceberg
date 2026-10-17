#include "catalog/rest/storage/aws.hpp"

#include "duckdb/common/http_util.hpp"
#include "duckdb/common/http_transport_manager.hpp"
#include "duckdb/common/encryption_state.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/main/client_data.hpp"

#include "iceberg_logging.hpp"
#include "catalog/rest/storage/iceberg_authorization.hpp"

namespace duckdb {

namespace {

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

//! Hex encoded SHA256 of the request body, as it appears in x-amz-content-sha256 and the canonical request.
string GetPayloadHash(EncryptionUtil &encryption_util, const string &data) {
	string result(CryptoHash::GetHexDigestSize(CryptoHashFunction::SHA256), '\0');
	encryption_util.HashHex(CryptoHashFunction::SHA256, const_data_ptr_cast(data.data()), data.size(), &result[0]);
	return result;
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

HTTPHeaders AWSInput::SignRequest(RequestType request_type, ClientContext &context, HTTPHeaders &headers,
                                  const string &data) const {
	auto &db = DatabaseInstance::GetDatabase(context);
	// the crypto module of httpfs if that is loaded, duckdb's own otherwise
	auto encryption_util = db.GetEncryptionUtil(true);

	auto timestamp = Timestamp::GetCurrentTimestamp();
	string date_now = StrfTimeFormat::Format(timestamp, "%Y%m%d");
	string datetime_now = StrfTimeFormat::Format(timestamp, "%Y%m%dT%H%M%SZ");
	auto payload_hash = GetPayloadHash(*encryption_util, data);

	// The headers that are signed and sent, in the (alphabetical) order of the canonical request.
	vector<std::pair<string, string>> signed_headers;
	if (headers.HasHeader("Content-Type")) {
		auto content_type = headers.GetHeaderValue("Content-Type");
		if (!content_type.empty()) {
			signed_headers.emplace_back("Content-Type", std::move(content_type));
		}
	}
	signed_headers.emplace_back("host", authority);
	signed_headers.emplace_back("x-amz-content-sha256", payload_hash);
	signed_headers.emplace_back("x-amz-date", datetime_now);
	if (!session_token.empty()) {
		signed_headers.emplace_back("x-amz-security-token", session_token);
	}
	if (headers.HasHeader("X-Iceberg-Access-Delegation")) {
		auto access_delegation = headers.GetHeaderValue("X-Iceberg-Access-Delegation");
		if (!access_delegation.empty()) {
			signed_headers.emplace_back("X-Iceberg-Access-Delegation", std::move(access_delegation));
		}
	}

	HTTPHeaders result(db);
	string canonical_headers;
	string signed_header_names;
	for (auto &header : signed_headers) {
		auto canonical_name = StringUtil::Lower(header.first);
		canonical_headers += canonical_name + ":" + header.second + "\n";
		if (!signed_header_names.empty()) {
			signed_header_names += ";";
		}
		signed_header_names += canonical_name;
		result[header.first] = header.second;
	}

	// it's unclear to be why we need to transform %2F into %252F, see
	// https://en.wikipedia.org/wiki/Percent-encoding#Percent_character
	auto canonical_path = StringUtil::Replace(CanonicalPath(), "%2F", "%252F");
	auto query_string = QueryString();
	auto canonical_query_string = query_string.empty() ? string() : query_string.substr(1);

	SignatureV4Params signature_params;
	signature_params.canonical_request = string(MethodName(request_type)) + "\n" + canonical_path + "\n" +
	                                     canonical_query_string + "\n" + canonical_headers + "\n" +
	                                     signed_header_names + "\n" + payload_hash;
	signature_params.credential_scope = date_now + "/" + region + "/" + service + "/aws4_request";
	signature_params.region = region;
	signature_params.service = service;
	signature_params.secret_access_key = secret;
	signature_params.date_now = date_now;
	signature_params.datetime_now = datetime_now;
	auto signature = HTTPUtil::CreateSignatureV4(*encryption_util, signature_params);

	result["Authorization"] = "AWS4-HMAC-SHA256 Credential=" + key_id + "/" + signature_params.credential_scope +
	                          ", SignedHeaders=" + signed_header_names + ", Signature=" + signature;
	return result;
}

unique_ptr<HTTPResponse> AWSInput::Request(RequestType request_type, ClientContext &context, HTTPHeaders &headers,
                                           const string &data) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto res = SignRequest(request_type, context, headers, data);

	string request_url = URL();
	auto session = db.config.GetHTTPTransportManager().CreateSession(context, request_url);
	auto &params = session.Parameters();

	switch (request_type) {
	case RequestType::HEAD_REQUEST: {
		HeadRequestInfo head_request(request_url, res, params);
		return session.Request(head_request);
	}
	case RequestType::DELETE_REQUEST: {
		DeleteRequestInfo delete_request(request_url, res, params);
		return session.Request(delete_request);
	}
	case RequestType::GET_REQUEST: {
		GetRequestInfo get_request(request_url, res, params, nullptr, nullptr);
		return session.Request(get_request);
	}
	case RequestType::POST_REQUEST: {
		PostRequestInfo post_request(request_url, res, params, reinterpret_cast<const_data_ptr_t>(data.c_str()),
		                             data.size());
		auto x = session.Request(post_request);
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
