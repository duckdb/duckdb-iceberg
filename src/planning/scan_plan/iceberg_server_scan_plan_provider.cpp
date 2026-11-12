#include "planning/scan_plan/iceberg_scan_plan_provider.hpp"
#include "duckdb/logging/logger.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {

ServerSideScanPlanProvider::ServerSideScanPlanProvider(IcebergServerSideScanPlan plan_p,
                                                       const IcebergTableMetadata &metadata_p, ClientContext &context_p)
    : plan(std::move(plan_p)), metadata(metadata_p), context(context_p), delete_indexes(plan.delete_manifests.size()) {
}

void ServerSideScanPlanProvider::LoadManifestList() {
}

void ServerSideScanPlanProvider::ReadDeleteManifests(const vector<idx_t> &manifest_indexes, idx_t filter_count) {
	lock_guard<mutex> guard(index_lock);
	idx_t builds = 0;
	for (auto manifest_idx : manifest_indexes) {
		if (manifest_idx >= delete_indexes.size()) {
			throw InternalException("Selected server-side delete manifest index %llu is out of bounds", manifest_idx);
		}
		if (!delete_indexes[manifest_idx]) {
			auto &manifest = plan.delete_manifests[manifest_idx];
			auto &spec = metadata.partition_specs.at(manifest.GetManifest().partition_spec_id);
			// Preserve the input if construction throws. Server plans are already materialized.
			delete_indexes[manifest_idx] =
			    make_shared_ptr<IcebergDeleteCandidateIndex>(context, manifest.GetManifestEntries(), spec);
			builds++;
		}
	}
	if (builds) {
		DUCKDB_LOG(context, IcebergLogType,
		           "Iceberg metadata phase=delete_candidate_index scope=server_provider delete_index_builds=%llu",
		           builds);
	}
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

vector<IcebergDeleteManifestHandle>
ServerSideScanPlanProvider::GetDeleteIndexes(const vector<idx_t> &manifest_indexes) {
	lock_guard<mutex> guard(index_lock);
	vector<IcebergDeleteManifestHandle> result;
	for (auto manifest_idx : manifest_indexes) {
		if (manifest_idx >= delete_indexes.size() || !delete_indexes[manifest_idx]) {
			throw InternalException("Selected server-side delete manifest %llu has no completed index", manifest_idx);
		}
		result.push_back({manifest_idx, delete_indexes[manifest_idx]});
	}
	return result;
}

bool ServerSideScanPlanProvider::TryGetNextBatch(IcebergDataViewCursor &cursor) {
	return cursor.has_current_batch || read_state.TryReadBatch(cursor);
}

void ServerSideScanPlanProvider::FinishScanTasks() {
}

bool ServerSideScanPlanProvider::DeleteFileAppliesToDataFile(const string &data_file_path,
                                                             const IcebergFileIdentity &delete_file) const {
	auto refs = plan.delete_files_by_data_file.find(data_file_path);
	return refs != plan.delete_files_by_data_file.end() && refs->second.count(delete_file);
}

const vector<IcebergManifestListEntry> &ServerSideScanPlanProvider::DataManifests() {
	return plan.data_manifests;
}

const vector<IcebergManifestListEntry> &ServerSideScanPlanProvider::DeleteManifests() {
	return plan.delete_manifests;
}

} // namespace duckdb
