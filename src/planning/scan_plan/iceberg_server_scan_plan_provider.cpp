#include "planning/scan_plan/iceberg_scan_plan_provider.hpp"

namespace duckdb {

ServerSideScanPlanProvider::ServerSideScanPlanProvider(IcebergServerSideScanPlan plan_p) : plan(std::move(plan_p)) {
}

void ServerSideScanPlanProvider::LoadManifestList() {
}

void ServerSideScanPlanProvider::ReadDeleteManifests(const vector<idx_t> &manifest_indexes, idx_t filter_count) {
}

void ServerSideScanPlanProvider::StartDataManifestScan(const vector<bool> &matching_manifests, idx_t filter_count) {
	if (data_manifest_scan_started) {
		return;
	}
	data_manifest_scan_started = true;
	for (idx_t i = 0; i < plan.data_manifests.size(); i++) {
		read_state.PushBatch(ManifestReadBatch {i, 0, plan.data_manifests[i].GetManifestEntries().size()});
	}
}

vector<IcebergDeleteFileReference> ServerSideScanPlanProvider::GetDeleteFiles(const vector<idx_t> &manifest_indexes) {
	vector<IcebergDeleteFileReference> result;
	for (auto manifest_idx : manifest_indexes) {
		if (manifest_idx >= plan.delete_manifests.size()) {
			throw InternalException("Selected server-side delete manifest index %llu is out of bounds", manifest_idx);
		}
		auto &manifest_list_entry = plan.delete_manifests[manifest_idx];
		auto &manifest_entries = manifest_list_entry.GetManifestEntries();
		for (idx_t entry_idx = 0; entry_idx < manifest_entries.size(); entry_idx++) {
			auto &manifest_entry = manifest_entries[entry_idx];
			if (manifest_entry.status != IcebergManifestEntryStatusType::DELETED) {
				result.push_back({manifest_idx, entry_idx});
			}
		}
	}
	return result;
}

bool ServerSideScanPlanProvider::TryGetNextBatch(IcebergDataViewCursor &cursor) {
	return cursor.has_current_batch || read_state.TryReadBatch(cursor);
}

void ServerSideScanPlanProvider::FinishScanTasks() {
}

bool ServerSideScanPlanProvider::DeleteFileAppliesToDataFile(const string &data_file_path,
                                                             const string &delete_file_path) const {
	auto refs = plan.delete_files_by_data_file.find(data_file_path);
	return refs != plan.delete_files_by_data_file.end() && refs->second.count(delete_file_path);
}

vector<IcebergManifestListEntry> &ServerSideScanPlanProvider::DataManifests() {
	return plan.data_manifests;
}

vector<IcebergManifestListEntry> &ServerSideScanPlanProvider::DeleteManifests() {
	return plan.delete_manifests;
}

} // namespace duckdb
