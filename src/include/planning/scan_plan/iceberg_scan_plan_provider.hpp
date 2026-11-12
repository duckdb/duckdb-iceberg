#pragma once

#include "catalog/rest/api/iceberg_scan_planning.hpp"
#include "planning/deletes/iceberg_delete_planner.hpp"
#include "planning/iceberg_manifest_read_state.hpp"
#include "planning/scan_plan/iceberg_scan_plan_state.hpp"
#include "planning/scan_plan/iceberg_manifest_store.hpp"

namespace duckdb {

class ClientContext;
class FileSystem;
class IcebergTableSchemaVersion;
class IcebergScanOrder;
struct IcebergTableFilters;
struct IcebergTransactionData;

class IcebergScanPlanProvider {
public:
	virtual ~IcebergScanPlanProvider() = default;

	static unique_ptr<IcebergScanPlanProvider>
	Create(IcebergScanPlanState &shared_state, IcebergScanPlanContext context,
	       optional_ptr<IcebergTableSchemaVersion> table_entry, const IcebergTableFilters &table_filters,
	       const IcebergScanOrder &scan_order, bool server_side_planning_enabled);

	virtual void LoadManifestList() = 0;
	virtual void StartDataManifestScan(const vector<bool> &matching_manifests, idx_t filter_count) = 0;
	virtual void ReadDeleteManifests(const vector<idx_t> &manifest_indexes, idx_t filter_count) = 0;
	virtual vector<IcebergDeleteFileReference> GetDeleteFiles(const vector<idx_t> &manifest_indexes) = 0;
	virtual bool TryGetNextBatch(IcebergDataViewCursor &cursor) = 0;
	virtual void FinishScanTasks() = 0;
	virtual bool DeleteFileAppliesToDataFile(const string &data_file_path,
	                                         const IcebergFileIdentity &delete_file) const = 0;
	virtual const vector<IcebergManifestListEntry> &DataManifests() = 0;
	virtual const vector<IcebergManifestListEntry> &DeleteManifests() = 0;
	virtual vector<reference<const IcebergManifestListEntry>> TransactionDataManifests() {
		return {};
	}
	virtual vector<reference<const IcebergManifestListEntry>> TransactionDeleteManifests() {
		return {};
	}
};

class ClientSideScanPlanProvider final : public IcebergScanPlanProvider {
public:
	ClientSideScanPlanProvider(IcebergScanPlanState &shared_state, IcebergScanPlanContext context);

	void LoadManifestList() override DUCKDB_REQUIRES(store.lock);
	void StartDataManifestScan(const vector<bool> &matching_manifests, idx_t filter_count) override
	    DUCKDB_REQUIRES(store.lock);
	void ReadDeleteManifests(const vector<idx_t> &manifest_indexes, idx_t filter_count) override;
	vector<IcebergDeleteFileReference> GetDeleteFiles(const vector<idx_t> &manifest_indexes) override
	    DUCKDB_REQUIRES(store.lock);
	bool TryGetNextBatch(IcebergDataViewCursor &cursor) override DUCKDB_REQUIRES(store.lock);
	void FinishScanTasks() override DUCKDB_REQUIRES(store.lock);
	bool DeleteFileAppliesToDataFile(const string &data_file_path,
	                                 const IcebergFileIdentity &delete_file) const override;
	const vector<IcebergManifestListEntry> &DataManifests() override DUCKDB_REQUIRES(store.lock);
	const vector<IcebergManifestListEntry> &DeleteManifests() override DUCKDB_REQUIRES(store.lock);

	vector<reference<const IcebergManifestListEntry>> TransactionDataManifests() override DUCKDB_REQUIRES(store.lock);
	vector<reference<const IcebergManifestListEntry>> TransactionDeleteManifests() override DUCKDB_REQUIRES(store.lock);

private:
	IcebergManifestStore &store;
};

class ServerSideScanPlanProvider final : public IcebergScanPlanProvider {
public:
	explicit ServerSideScanPlanProvider(IcebergServerSideScanPlan plan);

	void LoadManifestList() override;
	void StartDataManifestScan(const vector<bool> &matching_manifests, idx_t filter_count) override;
	void ReadDeleteManifests(const vector<idx_t> &manifest_indexes, idx_t filter_count) override;
	vector<IcebergDeleteFileReference> GetDeleteFiles(const vector<idx_t> &manifest_indexes) override;
	bool TryGetNextBatch(IcebergDataViewCursor &cursor) override;
	void FinishScanTasks() override;
	bool DeleteFileAppliesToDataFile(const string &data_file_path,
	                                 const IcebergFileIdentity &delete_file) const override;
	const vector<IcebergManifestListEntry> &DataManifests() override;
	const vector<IcebergManifestListEntry> &DeleteManifests() override;

private:
	//! Declared before parsed delete data so its manifest-entry references are destroyed first.
	IcebergServerSideScanPlan plan;
	ManifestEntryReadState read_state;
	bool data_manifest_scan_started = false;
};

} // namespace duckdb
