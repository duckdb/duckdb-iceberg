#pragma once

#include "catalog/rest/api/iceberg_scan_planning.hpp"
#include "planning/deletes/iceberg_delete_planner.hpp"
#include "planning/iceberg_manifest_read_state.hpp"
#include "planning/scan_plan/iceberg_scan_plan_state.hpp"
#include "planning/snapshot/iceberg_scan_info.hpp"

namespace duckdb {

class ClientContext;
class FileSystem;
class IcebergTableSchemaVersion;
class IcebergScanOrder;
struct IcebergTableFilters;
struct IcebergTransactionData;

struct IcebergDeleteFileReference {
	idx_t manifest_idx;
	idx_t entry_idx;
};

struct IcebergScanPlanContext {
	ClientContext &context;
	FileSystem &fs;
	const string &path;
	const IcebergOptions &options;
	const IcebergSnapshotScanInfo &snapshot;
	const IcebergTableMetadata &metadata;
	const IcebergTableSchema &schema;
	optional_ptr<const IcebergTransactionData> transaction_data;
};

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
	virtual bool DeleteFileAppliesToDataFile(const string &data_file_path, const string &delete_file_path) const = 0;
	virtual vector<IcebergManifestListEntry> &DataManifests() = 0;
	virtual vector<IcebergManifestListEntry> &DeleteManifests() = 0;
	virtual shared_ptr<IcebergDeleteFileLoadState> &GetDeleteFileLoad(IcebergDeleteFileReference delete_file) = 0;
	virtual position_delete_map_t &PositionalDeleteData() = 0;
};

class ClientSideScanPlanProvider final : public IcebergScanPlanProvider {
public:
	ClientSideScanPlanProvider(IcebergScanPlanState &shared_state, IcebergScanPlanContext context);

	void LoadManifestList() override DUCKDB_REQUIRES(shared_state.lock);
	void StartDataManifestScan(const vector<bool> &matching_manifests, idx_t filter_count) override
	    DUCKDB_REQUIRES(shared_state.lock);
	void ReadDeleteManifests(const vector<idx_t> &manifest_indexes, idx_t filter_count) override;
	vector<IcebergDeleteFileReference> GetDeleteFiles(const vector<idx_t> &manifest_indexes) override
	    DUCKDB_REQUIRES(shared_state.lock, shared_state.delete_lock);
	bool TryGetNextBatch(IcebergDataViewCursor &cursor) override DUCKDB_REQUIRES(shared_state.lock);
	void FinishScanTasks() override DUCKDB_REQUIRES(shared_state.lock);
	bool DeleteFileAppliesToDataFile(const string &data_file_path, const string &delete_file_path) const override;
	vector<IcebergManifestListEntry> &DataManifests() override DUCKDB_REQUIRES(shared_state.lock);
	vector<IcebergManifestListEntry> &DeleteManifests() override DUCKDB_REQUIRES(shared_state.lock);
	shared_ptr<IcebergDeleteFileLoadState> &GetDeleteFileLoad(IcebergDeleteFileReference delete_file) override
	    DUCKDB_REQUIRES(shared_state.lock, shared_state.delete_lock);
	position_delete_map_t &PositionalDeleteData() override DUCKDB_REQUIRES(shared_state.delete_lock);

private:
	IcebergScanPlanState &shared_state;
	IcebergScanPlanContext context;
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
	bool DeleteFileAppliesToDataFile(const string &data_file_path, const string &delete_file_path) const override;
	vector<IcebergManifestListEntry> &DataManifests() override;
	vector<IcebergManifestListEntry> &DeleteManifests() override;
	shared_ptr<IcebergDeleteFileLoadState> &GetDeleteFileLoad(IcebergDeleteFileReference delete_file) override;
	position_delete_map_t &PositionalDeleteData() override;

private:
	//! Declared before parsed delete data so its manifest-entry references are destroyed first.
	IcebergServerSideScanPlan plan;
	ManifestEntryReadState read_state;
	bool data_manifest_scan_started = false;
	vector<unordered_map<idx_t, shared_ptr<IcebergDeleteFileLoadState>>> delete_file_loads;
	position_delete_map_t positional_delete_data;
};

} // namespace duckdb
