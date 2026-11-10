#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/value.hpp"

#include "rest_catalog/objects/remote_signing_config.hpp"

namespace duckdb {
class ClientContext;
class IcebergCatalog;

//! Turns the remote signing information a catalog returns for a table into the options of the scoped 's3' secret
//! httpfs signs its requests through.
struct IcebergRemoteSigning {
	//! Table config keys a catalog uses to describe remote signing
	static constexpr const char *ENABLED_KEY = "s3.remote-signing-enabled";
	static constexpr const char *SIGNER_URI_KEY = "signer.uri";
	static constexpr const char *SIGNER_ENDPOINT_KEY = "signer.endpoint";
	//! Deprecated aliases of the keys above, still sent by older catalogs
	static constexpr const char *DEPRECATED_SIGNER_URI_KEY = "s3.signer.uri";
	static constexpr const char *DEPRECATED_SIGNER_ENDPOINT_KEY = "s3.signer.endpoint";
	//! The endpoint catalogs that set the deprecated keys expect when they leave it out
	static constexpr const char *DEPRECATED_DEFAULT_SIGNER_ENDPOINT = "v1/aws/s3/sign";

	//! Whether the catalog reports remote signing as enabled in this config
	static bool IsEnabled(const case_insensitive_map_t<string> &config);
	//! The endpoint httpfs sends this table's storage requests to for signing
	static string GetSignerURL(const IcebergCatalog &catalog, const vector<string> &namespace_items,
	                           const string &table_name, const case_insensitive_map_t<string> &table_config,
	                           bool has_remote_signing_config);
	//! Replace the credentials in a secret's options with the signer that signs its requests
	static void AddSecretOptions(ClientContext &context, IcebergCatalog &catalog, const vector<string> &namespace_items,
	                             const string &table_name, const string &log_table_name,
	                             const case_insensitive_map_t<string> &table_config,
	                             optional_ptr<const rest_api_objects::RemoteSigningConfig> signing_config,
	                             case_insensitive_map_t<Value> &options);
};

} // namespace duckdb
