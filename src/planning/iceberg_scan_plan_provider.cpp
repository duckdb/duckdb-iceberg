#include "planning/iceberg_scan_plan_provider.hpp"

namespace duckdb {

ClientSideScanPlanProvider::ClientSideScanPlanProvider(IcebergMultiFileListSharedState &shared_state_p)
    : shared_state(shared_state_p) {
}

bool ClientSideScanPlanProvider::IsServerSide() const {
	return false;
}

vector<IcebergManifestListEntry> &ClientSideScanPlanProvider::DataManifests() {
	return shared_state.committed_data_manifests;
}

vector<IcebergManifestListEntry> &ClientSideScanPlanProvider::DeleteManifests() {
	return shared_state.committed_delete_manifests;
}

ManifestEntryReadState &ClientSideScanPlanProvider::ReadState() {
	return shared_state.read_state;
}

bool &ClientSideScanPlanProvider::DataManifestScanStarted() {
	return shared_state.data_manifest_scan_started;
}

bool &ClientSideScanPlanProvider::DeleteEntriesEnumerated() {
	return shared_state.delete_entries_enumerated;
}

idx_t &ClientSideScanPlanProvider::NextDeleteEntryToProcess() {
	return shared_state.next_delete_entry_to_process;
}

vector<BoundIcebergManifestEntry> &ClientSideScanPlanProvider::DeleteManifestEntries() {
	return shared_state.delete_manifest_entries;
}

case_insensitive_map_t<shared_ptr<IcebergDeleteData>> &ClientSideScanPlanProvider::PositionalDeleteData() {
	return shared_state.positional_delete_data;
}

map<sequence_number_t, unique_ptr<IcebergEqualityDeleteData>> &ClientSideScanPlanProvider::EqualityDeleteData() {
	return shared_state.equality_delete_data;
}

const case_insensitive_map_t<unordered_set<string>> &ClientSideScanPlanProvider::DeleteFilesByDataFile() const {
	return empty_delete_files_by_data_file;
}

ServerSideScanPlanProvider::ServerSideScanPlanProvider(IcebergServerSideScanPlan plan_p) : plan(std::move(plan_p)) {
}

bool ServerSideScanPlanProvider::IsServerSide() const {
	return true;
}

vector<IcebergManifestListEntry> &ServerSideScanPlanProvider::DataManifests() {
	return plan.data_manifests;
}

vector<IcebergManifestListEntry> &ServerSideScanPlanProvider::DeleteManifests() {
	return plan.delete_manifests;
}

ManifestEntryReadState &ServerSideScanPlanProvider::ReadState() {
	return read_state;
}

bool &ServerSideScanPlanProvider::DataManifestScanStarted() {
	return data_manifest_scan_started;
}

bool &ServerSideScanPlanProvider::DeleteEntriesEnumerated() {
	return delete_entries_enumerated;
}

idx_t &ServerSideScanPlanProvider::NextDeleteEntryToProcess() {
	return next_delete_entry_to_process;
}

vector<BoundIcebergManifestEntry> &ServerSideScanPlanProvider::DeleteManifestEntries() {
	return delete_manifest_entries;
}

case_insensitive_map_t<shared_ptr<IcebergDeleteData>> &ServerSideScanPlanProvider::PositionalDeleteData() {
	return positional_delete_data;
}

map<sequence_number_t, unique_ptr<IcebergEqualityDeleteData>> &ServerSideScanPlanProvider::EqualityDeleteData() {
	return equality_delete_data;
}

const case_insensitive_map_t<unordered_set<string>> &ServerSideScanPlanProvider::DeleteFilesByDataFile() const {
	return plan.delete_files_by_data_file;
}

} // namespace duckdb
