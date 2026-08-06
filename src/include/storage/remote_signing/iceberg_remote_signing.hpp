//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/remote_signing/iceberg_remote_signing.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

//! Everything needed to turn an s3 path below a table location into a signed HTTP request
struct IcebergRemoteSigningTarget {
	string catalog_name;
	string signer_url;
	string region;
	//! Host (and optional port) of the S3 endpoint, without a scheme
	string host;
	bool use_ssl = true;
	bool path_style_access = false;
};

struct IcebergRemoteSigningConfig {
	//! The endpoint used when the catalog does not report one, as defined by the Iceberg REST spec
	static constexpr const char *DEFAULT_SIGNER_ENDPOINT = "v1/aws/s3/sign";

	//! Read the remote signing properties out of the 'config' of a LoadTableResult
	static bool TryParse(const case_insensitive_map_t<string> &config, const string &catalog_uri,
	                     const string &catalog_name, IcebergRemoteSigningTarget &result);
};

struct IcebergSignedRequest {
	string url;
	HTTPHeaders headers;
};

//! Signing targets and cached signatures, scoped to a single DatabaseInstance
class IcebergRemoteSigningRegistry {
public:
	//! Signatures are reused for this long, matching the Iceberg S3V4RestSignerClient cache
	static constexpr int64_t SIGNATURE_CACHE_MS = 30000;

public:
	void RegisterTarget(const string &location, IcebergRemoteSigningTarget target);
	bool TryGetTarget(const string &path, IcebergRemoteSigningTarget &result);

	bool TryGetSignature(const string &cache_key, IcebergSignedRequest &result);
	void PutSignature(const string &cache_key, const IcebergSignedRequest &signature);

private:
	struct CachedSignature {
		timestamp_t expires_at;
		IcebergSignedRequest signature;
	};

private:
	mutex lock;
	unordered_map<string, IcebergRemoteSigningTarget> targets;
	unordered_map<string, CachedSignature> signatures;
};

struct IcebergStorageExtensionInfo : public StorageExtensionInfo {
	explicit IcebergStorageExtensionInfo(shared_ptr<IcebergRemoteSigningRegistry> remote_signing_p)
	    : remote_signing(std::move(remote_signing_p)) {
	}

	shared_ptr<IcebergRemoteSigningRegistry> remote_signing;
};

} // namespace duckdb
