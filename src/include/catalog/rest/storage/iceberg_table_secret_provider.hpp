#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/secret/secret.hpp"

namespace duckdb {
class ExtensionLoader;
class ClientContext;
class BaseSecret;
class IcebergCatalog;
struct SecretEntry;

//! Secret provider for Iceberg vended table credentials.
//!
//! Storage secrets created from vended credentials use this provider so httpfs'
//! refresh path can dispatch back into Iceberg. On refresh, the provider
//! reloads the table from the Iceberg REST catalog and replaces the expired
//! scoped storage secret with newly vended credentials.
class IcebergTableSecretProvider {
public:
	static const char *Provider();
	static bool SupportsStorageType(const string &storage_type);
	static void Register(ExtensionLoader &loader);
	static void AddHTTPSecretsToOptions(SecretEntry &http_secret_entry, case_insensitive_map_t<Value> &options);
	static unique_ptr<SecretEntry> GetHTTPSecretForCatalog(ClientContext &context, IcebergCatalog &catalog);
	static Value MakeRefreshInfo(const string &catalog_name, const string &schema_name, const string &table_name);
	static unique_ptr<BaseSecret> CreateSecret(ClientContext &context, CreateSecretInput &input);
};

} // namespace duckdb
