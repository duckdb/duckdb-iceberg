#include "planning/scan_plan/iceberg_scan_plan_provider.hpp"
#include "planning/scan_plan/iceberg_manifest_store.hpp"

namespace duckdb {

ClientSideScanPlanProvider::ClientSideScanPlanProvider(IcebergScanPlanState &shared_state,
                                                       IcebergScanPlanContext context)
    : store(shared_state.GetManifestStore(std::move(context))) {
}

void ClientSideScanPlanProvider::LoadManifestList() {
	store.LoadManifestList();
}

void ClientSideScanPlanProvider::StartDataManifestScan(const vector<bool> &matching_manifests, idx_t filter_count) {
	store.StartDataManifestScan(matching_manifests, filter_count);
}

void ClientSideScanPlanProvider::ReadDeleteManifests(const vector<idx_t> &manifest_indexes, idx_t filter_count) {
	store.ReadDeleteManifests(manifest_indexes, filter_count);
}

vector<IcebergDeleteFileReference> ClientSideScanPlanProvider::GetDeleteFiles(const vector<idx_t> &manifest_indexes) {
	return store.GetDeleteFiles(manifest_indexes);
}

bool ClientSideScanPlanProvider::TryGetNextBatch(IcebergDataViewCursor &cursor) {
	return store.TryGetNextBatch(cursor);
}

void ClientSideScanPlanProvider::FinishScanTasks() {
	store.FinishScanTasks();
}

const vector<IcebergManifestListEntry> &ClientSideScanPlanProvider::DataManifests() {
	return store.DataManifests();
}

const vector<IcebergManifestListEntry> &ClientSideScanPlanProvider::DeleteManifests() {
	return store.DeleteManifests();
}

vector<reference<const IcebergManifestListEntry>> ClientSideScanPlanProvider::TransactionDataManifests() {
	return store.TransactionDataManifests();
}

vector<reference<const IcebergManifestListEntry>> ClientSideScanPlanProvider::TransactionDeleteManifests() {
	return store.TransactionDeleteManifests();
}

bool ClientSideScanPlanProvider::DeleteFileAppliesToDataFile(const string &data_file_path,
                                                             const IcebergFileIdentity &delete_file) const {
	return true;
}

} // namespace duckdb
