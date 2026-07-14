//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/rest/iceberg_access_delegation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;
class IcebergCatalog;
class IcebergTableSchema;
struct IcebergTableInformation;
struct IcebergAttachOptions;
struct IcebergScanInfo;
struct IcebergMultiFileList;
struct IcebergPartitionInfo;
struct IRCAPITableCredentials;

//! Opaque, provider-owned per-table state. Iceberg stores it on IcebergTableInformation
//! without knowing its concrete type; a delegation provider downcasts it to its own state.
struct IcebergDelegationTableState {
	virtual ~IcebergDelegationTableState() = default;
};

//! Everything a delegation provider needs to act on a single table.
struct IcebergAccessDelegationContext {
	IcebergCatalog &catalog;
	IcebergTableInformation &table_info;
	ClientContext &context;
};

//! Extension point for "access delegation" on an attached Iceberg (REST) catalog.
//!
//! An access-delegation provider decides how object-storage credentials are obtained for a
//! scan, and may enforce mandatory scan filters. The iceberg extension itself ships no
//! provider; external extensions (e.g. the `lake_formation` community extension) register
//! one. The interface is intentionally free of any AWS/cloud-specific types so that iceberg
//! can drop direct cloud SDK dependencies over time without affecting providers.
class IcebergAccessDelegationProvider {
public:
	virtual ~IcebergAccessDelegationProvider() = default;

	//! The provider name. Selected via `ATTACH ... (ACCESS_DELEGATION_MODE '<name>')`. Iceberg
	//! autoloads the extension whose name matches this when no provider is registered yet.
	virtual string GetProviderName() const = 0;

	//! Validate (and consume) provider-specific attach options. Provider-specific keys arrive in
	//! `options.options`; the provider should read and erase the ones it owns so iceberg's
	//! "unhandled options" check does not reject them.
	virtual void ValidateAttachOptions(IcebergAttachOptions &options) = 0;

	//! Called once a table's Iceberg metadata is available (cache hit or fresh load). Providers
	//! load and cache any external policy here, typically into `table_info` delegation state.
	virtual void OnTableLoaded(IcebergAccessDelegationContext &dctx) = 0;

	//! Produce object-storage credentials for a scan, as a DuckDB secret definition. When
	//! `partition_values` is set, credentials should be scoped to that partition.
	virtual IRCAPITableCredentials GetScanCredentials(IcebergAccessDelegationContext &dctx,
	                                                  optional_ptr<const vector<Value>> partition_values) = 0;

	//! Apply any mandatory scan filters for this table. Providers may push column filters onto the
	//! file list and store a bound filter on `scan_info` (see IcebergScanInfo::mandatory_delegation_filter_*),
	//! which iceberg re-applies above the scan as a safety net.
	virtual void ApplyMandatoryScanFilters(IcebergAccessDelegationContext &dctx, IcebergScanInfo &scan_info,
	                                       IcebergMultiFileList &file_list, const IcebergTableSchema &schema) = 0;

	//! Called while enumerating data files. Providers that vend per-partition credentials can refresh
	//! the relevant secret here.
	virtual void OnPartitionFile(IcebergAccessDelegationContext &dctx,
	                             const vector<IcebergPartitionInfo> &partition_info, const string &file_path) = 0;
};

//! Database-wide registry of access-delegation providers. Stored on DuckDB's ObjectCache so that
//! the iceberg extension and provider extensions (which are separate loadable binaries) can share it.
//!
//! Every method is defined inline on purpose: a provider extension is linked only against DuckDB
//! core, not against the iceberg extension, so it can call these (and DuckDB-core symbols) but not
//! iceberg's out-of-line functions. Shared state therefore flows exclusively through DuckDB-core
//! containers (ObjectCache, shared_ptr) and virtual dispatch on IcebergAccessDelegationProvider.
class IcebergPluginRegistry : public ObjectCacheEntry {
public:
	void RegisterProvider(shared_ptr<IcebergAccessDelegationProvider> provider) {
		lock_guard<mutex> guard(lock);
		providers[provider->GetProviderName()] = std::move(provider);
	}
	optional_ptr<IcebergAccessDelegationProvider> GetProvider(const string &provider_name) {
		lock_guard<mutex> guard(lock);
		auto entry = providers.find(provider_name);
		if (entry == providers.end()) {
			return nullptr;
		}
		return entry->second.get();
	}

	//! ObjectCacheEntry interface
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		// Invalid index => non-evictable: the registry must outlive any scan.
		return optional_idx();
	}
	static string ObjectType() {
		return "iceberg_access_delegation_registry";
	}

	//! Fetch (creating if absent) the registry for this database.
	static IcebergPluginRegistry &GetOrCreate(ClientContext &context) {
		auto &cache = ObjectCache::GetObjectCache(context);
		return *cache.GetOrCreate<IcebergPluginRegistry>(ObjectType());
	}
	//! Fetch the registry if it exists, else nullptr.
	static optional_ptr<IcebergPluginRegistry> TryGet(ClientContext &context) {
		auto &cache = ObjectCache::GetObjectCache(context);
		return cache.Get<IcebergPluginRegistry>(ObjectType()).get();
	}
	//! DatabaseInstance overload, usable from an extension's Load() (no ClientContext required). Reaches
	//! the same per-database ObjectCache as the ClientContext overloads.
	static IcebergPluginRegistry &GetOrCreate(DatabaseInstance &db) {
		auto &cache = db.GetObjectCache();
		return *cache.GetOrCreate<IcebergPluginRegistry>(ObjectType());
	}

private:
	mutex lock;
	case_insensitive_map_t<shared_ptr<IcebergAccessDelegationProvider>> providers;
};

//! Free functions used by iceberg's catalog/scan code to dispatch to the active provider (if any).
//! All are no-ops when the attached catalog has no delegation provider configured.
struct IcebergAccessDelegation {
	//! Returns the provider configured for `catalog` (by attach option), or nullptr.
	static optional_ptr<IcebergAccessDelegationProvider> GetActiveProvider(IcebergCatalog &catalog,
	                                                                       ClientContext &context);
	//! Resolve and validate the provider during ATTACH, autoloading its extension if needed.
	static void ResolveProviderForAttach(IcebergAttachOptions &options, ClientContext &context);
	//! Dispatch IcebergAccessDelegationProvider::OnTableLoaded for the active provider.
	static void OnTableLoaded(IcebergTableInformation &table_info, ClientContext &context);
	//! Create the scan credential secret via the active provider. Returns true if a provider handled it.
	static bool PrepareScanCredentials(IcebergTableInformation &table_info, ClientContext &context,
	                                   optional_ptr<const vector<Value>> partition_values);
	//! Dispatch IcebergAccessDelegationProvider::ApplyMandatoryScanFilters for the active provider.
	static void ApplyMandatoryScanFilters(IcebergTableInformation &table_info, ClientContext &context,
	                                      IcebergScanInfo &scan_info, IcebergMultiFileList &file_list,
	                                      const IcebergTableSchema &schema);
	//! Dispatch IcebergAccessDelegationProvider::OnPartitionFile for the active provider.
	static void OnPartitionFile(IcebergTableInformation &table_info, ClientContext &context,
	                            const vector<IcebergPartitionInfo> &partition_info, const string &file_path);
};

} // namespace duckdb
