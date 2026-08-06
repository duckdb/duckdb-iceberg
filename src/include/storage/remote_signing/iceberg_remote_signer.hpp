//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/remote_signing/iceberg_remote_signer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/http_util.hpp"
#include "duckdb/main/client_context.hpp"

#include "storage/remote_signing/iceberg_remote_signing.hpp"

namespace duckdb {

class IcebergRemoteSigner {
public:
	//! Translate an s3://, s3a:// or s3n:// path into the HTTP url the object is reachable at
	static string ToHttpUrl(const IcebergRemoteSigningTarget &target, const string &path, string &host);
	//! Ask the catalog to sign a request, reusing a cached signature when one is still valid
	static IcebergSignedRequest Sign(ClientContext &context, IcebergRemoteSigningRegistry &registry,
	                                 const IcebergRemoteSigningTarget &target, RequestType request_type,
	                                 const string &http_url, const string &host);
};

} // namespace duckdb
