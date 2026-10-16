#pragma once

#include "duckdb/common/mutex.hpp"
#include "iceberg_options.hpp"
#include "planning/snapshot/iceberg_scan_info.hpp"

namespace duckdb {

class IcebergTableSchemaVersion;
class IcebergManifestStore;
class IcebergScanPlanProvider;
class IcebergScanOrder;
struct IcebergTableFilters;
struct IcebergScanPlanContext;

//! Scan identity and options, configured during binding and frozen before planning or view creation.
//! Consumers only receive a const reference; manifest caches are owned separately by IcebergManifestStore.
struct IcebergScanConfiguration {
	shared_ptr<IcebergScanInfo> scan_info;
	string path;
	optional_ptr<IcebergTableSchemaVersion> table;
	IcebergOptions options;
	bool server_side_planning_enabled = true;
	bool row_ids_required = false;
};

//! Metadata-planning state shared by filtered views of one Iceberg scan.
//! Delete-file contents and filters created while executing a task are intentionally
//! kept outside this state.
struct IcebergScanPlanState {
	IcebergScanPlanState(ClientContext &context, shared_ptr<IcebergScanInfo> scan_info, string path,
	                     const IcebergOptions &options);
	~IcebergScanPlanState();

	ClientContext &context;
	FileSystem &fs;

private:
	//! Outlives the manifest caches and readers that reference scan metadata.
	IcebergScanConfiguration configuration;

public:
	const IcebergScanConfiguration &Configuration() const;
	void SetScanInfo(shared_ptr<IcebergScanInfo> scan_info) DUCKDB_REQUIRES(lock);
	void SetOptions(const IcebergOptions &options) DUCKDB_REQUIRES(lock);
	void SetTable(IcebergTableSchemaVersion &table) DUCKDB_REQUIRES(lock);
	void DisableServerSidePlanning() DUCKDB_REQUIRES(lock);
	void RequireRowIds() DUCKDB_REQUIRES(lock);
	void FreezeConfiguration() DUCKDB_REQUIRES(lock);
	//! The first planning request selects the provider; subsequent filtered views reuse it.
	IcebergScanPlanProvider &GetScanPlanProvider(const IcebergTableFilters &filters, const IcebergScanOrder &scan_order)
	    DUCKDB_REQUIRES(lock);
	IcebergManifestStore &GetManifestStore(IcebergScanPlanContext context) DUCKDB_REQUIRES(lock);

	mutable annotated_mutex lock;
	//! FIXME: these are only used by deletes, we should find a better way to do this
	mutable unordered_map<string, IcebergPartition> data_file_partitions DUCKDB_GUARDED_BY(lock);

private:
	void RequireConfigurable() const DUCKDB_REQUIRES(lock);
	bool configuration_frozen DUCKDB_GUARDED_BY(lock) = false;
	unique_ptr<IcebergManifestStore> manifest_store DUCKDB_GUARDED_BY(lock);
	//! Destroy the provider before the manifest store it may reference.
	unique_ptr<IcebergScanPlanProvider> scan_plan_provider DUCKDB_GUARDED_BY(lock);
};

} // namespace duckdb
