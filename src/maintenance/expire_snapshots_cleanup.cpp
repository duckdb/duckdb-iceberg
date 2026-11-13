#include "maintenance/expire_snapshots_cleanup.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/snapshot/iceberg_snapshot_scan_info.hpp"
#include "iceberg_options.hpp"

#include <algorithm>

namespace duckdb {

namespace {

//! Only file paths are read, so a snapshot without a schema-id reads with the current schema.
IcebergSnapshotScanInfo ScanInfoFor(const IcebergTableMetadata &metadata, const IcebergSnapshot &snapshot) {
	IcebergSnapshotScanInfo snapshot_info;
	snapshot_info.snapshot = &snapshot;
	snapshot_info.schema_id = snapshot.HasSchemaId() ? snapshot.GetSchemaId() : metadata.GetCurrentSchemaId();
	return snapshot_info;
}

void ReadManifestList(ClientContext &context, const IcebergTableMetadata &metadata, const IcebergSnapshot &snapshot,
                      vector<IcebergManifestListEntry> &result) {
	IcebergManifestList::LoadManifestFiles(ScanInfoFor(metadata, snapshot), metadata, context, result);
}

//! The file paths of the manifest's live entries (ADDED and EXISTING). DELETED entries are tombstones of
//! files that other manifests may still list.
void ReadLivePaths(ClientContext &context, const IcebergTableMetadata &metadata, const IcebergSnapshot &snapshot,
                   const IcebergManifestListEntry &manifest, unordered_set<string> &paths) {
	vector<IcebergManifestListEntry> manifest_files;
	manifest_files.push_back(manifest);
	manifest_files[0].ResetManifestEntries();
	IcebergOptions options;
	auto &fs = FileSystem::GetFileSystem(context);
	auto scan =
	    AvroScan::ScanManifest(ScanInfoFor(metadata, snapshot), manifest_files, options, fs, "", metadata, context);
	auto reader = make_uniq<manifest_file::ManifestReader>(*scan);
	while (!reader->Finished()) {
		reader->Read();
	}
	scan->Complete();
	for (auto &entry : manifest_files[0].GetManifestEntries()) {
		if (entry.status != IcebergManifestEntryStatusType::DELETED) {
			paths.insert(entry.data_file.file_path);
		}
	}
}

vector<string> Sorted(const unordered_set<string> &paths) {
	vector<string> result(paths.begin(), paths.end());
	std::sort(result.begin(), result.end());
	return result;
}

} // namespace

ExpiredSnapshotFiles PlanExpiredSnapshotFiles(ClientContext &context, const IcebergTableMetadata &before,
                                              const vector<IcebergSnapshot> &expired_snapshots,
                                              const IcebergTableMetadata &after) {
	ExpiredSnapshotFiles result;
	unordered_set<int64_t> expired_ids;
	unordered_set<string> manifest_lists;
	//! Manifests of the expired snapshots, with the snapshot they were read from (for the schema)
	vector<pair<IcebergManifestListEntry, reference<const IcebergSnapshot>>> candidates;
	for (auto &snapshot : expired_snapshots) {
		if (after.snapshots.count(snapshot.snapshot_id)) {
			//! The catalog kept it; nothing of it may go.
			continue;
		}
		expired_ids.insert(snapshot.snapshot_id);
		if (!snapshot.manifest_list.empty()) {
			manifest_lists.insert(snapshot.manifest_list);
		}
		vector<IcebergManifestListEntry> manifests;
		ReadManifestList(context, before, snapshot, manifests);
		for (auto &manifest : manifests) {
			candidates.emplace_back(std::move(manifest), snapshot);
		}
	}
	if (expired_ids.empty()) {
		return result;
	}

	//! Everything a retained snapshot still lists stays, whichever snapshot added it.
	unordered_set<string> current_manifest_paths;
	vector<pair<IcebergManifestListEntry, reference<const IcebergSnapshot>>> current_manifests;
	for (auto &entry : after.snapshots) {
		vector<IcebergManifestListEntry> manifests;
		ReadManifestList(context, after, entry.second, manifests);
		for (auto &manifest : manifests) {
			if (current_manifest_paths.insert(manifest.GetFile().manifest_path).second) {
				current_manifests.emplace_back(std::move(manifest), entry.second);
			}
		}
	}

	unordered_set<string> manifests_to_delete;
	unordered_set<string> data_files;
	for (auto &candidate : candidates) {
		auto &path = candidate.first.GetFile().manifest_path;
		if (current_manifest_paths.count(path) || !manifests_to_delete.insert(path).second) {
			continue;
		}
		ReadLivePaths(context, before, candidate.second.get(), candidate.first, data_files);
	}
	if (!data_files.empty()) {
		unordered_set<string> live_paths;
		for (auto &manifest : current_manifests) {
			ReadLivePaths(context, after, manifest.second.get(), manifest.first, live_paths);
		}
		for (auto &path : live_paths) {
			data_files.erase(path);
		}
	}

	unordered_set<string> statistics_files;
	unordered_set<string> retained_statistics;
	for (auto &entry : after.statistics) {
		retained_statistics.insert(entry.path);
	}
	for (auto &entry : after.partition_statistics) {
		retained_statistics.insert(entry.path);
	}
	for (auto &entry : before.statistics) {
		if (expired_ids.count(entry.snapshot_id) && !retained_statistics.count(entry.path)) {
			statistics_files.insert(entry.path);
		}
	}
	for (auto &entry : before.partition_statistics) {
		if (expired_ids.count(entry.snapshot_id) && !retained_statistics.count(entry.path)) {
			statistics_files.insert(entry.path);
		}
	}

	result.data_files = Sorted(data_files);
	result.manifests = Sorted(manifests_to_delete);
	result.manifest_lists = Sorted(manifest_lists);
	result.statistics_files = Sorted(statistics_files);
	return result;
}

} // namespace duckdb
