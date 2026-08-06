#include "storage/remote_signing/iceberg_remote_signer.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/json_document.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"

#include "catalog/rest/api/url_utils.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {

static const char *ToMethodString(RequestType request_type) {
	switch (request_type) {
	case RequestType::GET_REQUEST:
		return "GET";
	case RequestType::HEAD_REQUEST:
		return "HEAD";
	case RequestType::PUT_REQUEST:
		return "PUT";
	case RequestType::POST_REQUEST:
		return "POST";
	case RequestType::DELETE_REQUEST:
		return "DELETE";
	default:
		throw NotImplementedException("Iceberg remote signing does not support requests of type %s",
		                              EnumUtil::ToString(request_type));
	}
}

static void SplitS3Path(const string &path, string &bucket, string &key) {
	auto scheme_end = path.find("://");
	if (scheme_end == string::npos) {
		throw InvalidInputException("Expected an S3 path, got '%s'", path);
	}
	auto bucket_start = scheme_end + 3;
	auto bucket_end = path.find('/', bucket_start);
	if (bucket_end == string::npos) {
		throw InvalidInputException("S3 path '%s' does not contain a key", path);
	}
	bucket = path.substr(bucket_start, bucket_end - bucket_start);
	key = path.substr(bucket_end + 1);
}

string IcebergRemoteSigner::ToHttpUrl(const IcebergRemoteSigningTarget &target, const string &path, string &host) {
	string bucket;
	string key;
	SplitS3Path(path, bucket, key);

	//! The signer computes the canonical request over the url as it is sent, so the key has to be
	//! percent-encoded here and left untouched afterwards
	auto encoded_key = StringUtil::URLEncode(key, false);
	const char *scheme = target.use_ssl ? "https://" : "http://";
	if (target.path_style_access) {
		host = target.host;
		return StringUtil::Format("%s%s%s/%s/%s", scheme, host, target.path_prefix, bucket, encoded_key);
	}
	host = bucket + "." + target.host;
	return StringUtil::Format("%s%s%s/%s", scheme, host, target.path_prefix, encoded_key);
}

static string BuildSignRequestBody(const IcebergRemoteSigningTarget &target, const char *method, const string &url,
                                   const string &host) {
	JSONWriter writer;
	auto root = writer.CreateObject();
	root.AddString("region", target.region);
	root.AddString("uri", url);
	root.AddString("method", method);

	auto headers = writer.CreateObject();
	auto host_values = writer.CreateArray();
	host_values.AppendString(host);
	headers.Add("Host", host_values);
	root.Add("headers", headers);

	root.Add("properties", writer.CreateObject());
	root.Add("body", writer.CreateNull());
	writer.SetRoot(root);
	return writer.ToString();
}

static IcebergSignedRequest ParseSignResponse(const string &body, const string &fallback_url) {
	auto doc = JSONDocument::Parse(body.c_str(), body.size());
	auto root = doc->GetRoot();
	if (!root.IsObject()) {
		throw HTTPException(StringUtil::Format("Iceberg remote signing response is not a JSON object: %s", body));
	}

	IcebergSignedRequest result;
	auto uri = root.GetMember("uri");
	result.url = uri.IsValid() && uri.IsString() ? uri.GetString() : fallback_url;

	auto headers = root.GetMember("headers");
	if (!headers.IsValid() || !headers.IsObject()) {
		throw HTTPException(StringUtil::Format("Iceberg remote signing response does not contain 'headers': %s", body));
	}
	headers.IterateObject([&result](const string &key, JSONValue value) {
		//! The signer reports its own caching policy through Cache-Control, it is not part of the signature
		if (StringUtil::CIEquals(key, "Cache-Control")) {
			return;
		}
		if (value.IsString()) {
			result.headers.Insert(key, value.GetString());
			return;
		}
		if (!value.IsArray()) {
			return;
		}
		value.IterateArray([&result, &key](JSONValue element) {
			if (element.IsString()) {
				result.headers.Insert(key, element.GetString());
			}
		});
	});
	return result;
}

IcebergSignedRequest IcebergRemoteSigner::Sign(ClientContext &context, IcebergRemoteSigningRegistry &registry,
                                               const IcebergRemoteSigningTarget &target, RequestType request_type,
                                               const string &http_url, const string &host) {
	auto method = ToMethodString(request_type);
	auto cache_key = StringUtil::Format("%s %s %s", target.signer_url, method, http_url);

	IcebergSignedRequest cached;
	if (registry.TryGetSignature(cache_key, cached)) {
		return cached;
	}

	auto &catalog = Catalog::GetCatalog(context, Identifier(target.catalog_name));
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();

	auto body = BuildSignRequestBody(target, method, http_url, host);
	auto endpoint_builder = IRCEndpointBuilder::FromURL(target.signer_url);
	HTTPHeaders headers(*context.db);
	headers.Insert("Content-Type", "application/json");

	DUCKDB_LOG(context, IcebergLogType, "Requesting an S3 signature from %s for %s %s", target.signer_url, method,
	           http_url);
	auto response =
	    ic_catalog.auth_handler->Request(RequestType::POST_REQUEST, context, endpoint_builder, headers, body);
	if (!response) {
		throw HTTPException(
		    StringUtil::Format("Failed to sign %s %s through the Iceberg catalog signer at %s: no response", method,
		                       http_url, target.signer_url));
	}
	if (response->status != HTTPStatusCode::OK_200) {
		throw HTTPException(*response, "Failed to sign %s %s through the Iceberg catalog signer at %s: %s", method,
		                    http_url, target.signer_url, response->body);
	}

	auto signature = ParseSignResponse(response->body, http_url);
	registry.PutSignature(cache_key, signature);
	return signature;
}

} // namespace duckdb
