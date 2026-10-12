#include "catalog/rest/transaction/iceberg_table_purge.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"

#include "common/iceberg_file_cleanup.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "iceberg_logging.hpp"
#include "iceberg_options.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/snapshot/iceberg_snapshot_scan_info.hpp"

namespace duckdb {

namespace {

//! Parsed the way Iceberg's PropertyUtil.propertyAsBoolean does: absent means enabled, and only (case-insensitive)
//! "true" means true.
bool GarbageCollectionEnabled(const IcebergTableMetadata &metadata) {
	auto property = metadata.table_properties.find("gc.enabled");
	if (property == metadata.table_properties.end()) {
		return true;
	}
	return StringUtil::CIEquals(property->second, "true");
}

void AddUnique(vector<string> &result, unordered_set<string> &seen, const string &path) {
	if (!path.empty() && seen.insert(path).second) {
		result.push_back(path);
	}
}

//! Purge needs only the required data_file.file_path from every row of every data and delete manifest, including
//! DELETED entries. A dedicated projection keeps collection independent of table schemas and partition values:
//! buildReplacement can reuse a field ID with an incompatible type while preserving the old snapshots and manifests.
//! The shared Avro scan plumbing carries snapshot_info; FILE_PATHS mode never dereferences its schema.
void ScanManifestFilePaths(ClientContext &context, const IcebergTableMetadata &metadata,
                           vector<IcebergManifestListEntry> &manifests, vector<string> &file_paths) {
	IcebergSnapshotScanInfo manifest_scan_info;
	manifest_scan_info.snapshot = metadata.GetLatestSnapshot();
	if (!manifest_scan_info.snapshot) {
		manifest_scan_info.snapshot = metadata.snapshots.begin()->second;
	}
	manifest_scan_info.schema_id = metadata.GetCurrentSchemaId();
	IcebergOptions options;
	auto &fs = FileSystem::GetFileSystem(context);
	auto scan = AvroScan::ScanManifestFilePaths(manifest_scan_info, manifests, options, fs, metadata.GetLocation(),
	                                            metadata, context, file_paths);
	manifest_file::ManifestReader reader(*scan);
	while (!reader.Finished()) {
		reader.Read();
	}
}

} // namespace

IcebergTableFiles IcebergTablePurge::CollectFiles(ClientContext &context, const IcebergTableMetadata &metadata,
                                                  const string &operation) {
	IcebergTableFiles files;

	//! Metadata that cannot be read is logged, and what it would have named stays behind. For a manifest that is what
	//! CatalogUtil.dropTableData does (suppressFailureWhenFinished, "to complete as much of the delete work as
	//! possible"). For a manifest list it is a deliberate deviation: dropTableData reads the manifest lists up front,
	//! outside that protection, so there an unreadable one fails the drop after the catalog already deleted the table,
	//! with nothing deleted. Skipping the snapshot instead follows the stated intent. Cancellation and fatal errors are
	//! not best effort and propagate.

	//! The manifest list of every snapshot, and every manifest once: snapshots share the manifests that were not
	//! rewritten between them.
	unordered_set<string> seen_manifests;
	vector<IcebergManifestListEntry> manifests;
	for (auto &entry : metadata.snapshots) {
		auto &snapshot = entry.second;
		IcebergSnapshotScanInfo snapshot_info;
		snapshot_info.snapshot = snapshot;
		snapshot_info.schema_id = snapshot.GetSchemaId();
		if (!snapshot.manifest_list.empty()) {
			files.manifest_lists.push_back(snapshot.manifest_list);
		}
		vector<IcebergManifestListEntry> snapshot_manifests;
		try {
			IcebergManifestList::LoadManifestFiles(snapshot_info, metadata, context, snapshot_manifests);
		} catch (std::exception &ex) {
			ErrorData error(ex);
			if (IcebergUtils::MustPropagate(error)) {
				throw;
			}
			context.InterruptCheck();
			DUCKDB_LOG_WARNING(
			    context,
			    "%s, the manifest list of snapshot %lld cannot be read, the manifests it names and their "
			    "data files may be left behind [%s]: %s",
			    operation, static_cast<int64_t>(snapshot.snapshot_id),
			    IcebergLogType::Redact(context, snapshot.manifest_list),
			    IcebergLogType::Redact(context, error.RawMessage()));
			continue;
		}
		for (auto &manifest : snapshot_manifests) {
			auto &path = manifest.GetFile().manifest_path;
			if (seen_manifests.insert(path).second) {
				files.manifests.push_back(path);
				manifests.push_back(std::move(manifest));
			}
		}
	}

	if (!manifests.empty()) {
		if (GarbageCollectionEnabled(metadata)) {
			vector<string> manifest_file_paths;
			try {
				ScanManifestFilePaths(context, metadata, manifests, manifest_file_paths);
			} catch (std::exception &ex) {
				ErrorData error(ex);
				if (IcebergUtils::MustPropagate(error)) {
					throw;
				}
				context.InterruptCheck();
				//! One scan reads the manifests in parallel; when it fails, read them one at a time to keep the paths
				//! of every manifest that can be read and to name the ones that cannot.
				manifest_file_paths.clear();
				for (auto &manifest : manifests) {
					vector<IcebergManifestListEntry> single_manifest {manifest};
					vector<string> single_manifest_paths;
					try {
						ScanManifestFilePaths(context, metadata, single_manifest, single_manifest_paths);
					} catch (std::exception &single_ex) {
						ErrorData single_error(single_ex);
						if (IcebergUtils::MustPropagate(single_error)) {
							throw;
						}
						context.InterruptCheck();
						DUCKDB_LOG_WARNING(
						    context,
						    "%s, a manifest cannot be read, the data files it names may be left behind [%s]: %s",
						    operation, IcebergLogType::Redact(context, manifest.GetFile().manifest_path),
						    IcebergLogType::Redact(context, single_error.RawMessage()));
						continue;
					}
					for (auto &path : single_manifest_paths) {
						manifest_file_paths.push_back(std::move(path));
					}
				}
			}
			unordered_set<string> seen_files;
			for (auto &path : manifest_file_paths) {
				AddUnique(files.data_files, seen_files, path);
			}
		} else {
			//! Deleting the data files could corrupt another table that references them; the metadata is still
			//! exclusively this table's.
			files.data_files_retained = true;
		}
	}

	unordered_set<string> seen_statistics;
	for (auto &path : metadata.statistics_files) {
		AddUnique(files.statistics_files, seen_statistics, path);
	}
	for (auto &path : metadata.partition_statistics_files) {
		AddUnique(files.statistics_files, seen_statistics, path);
	}

	//! Oldest first, the current metadata file last, so an interrupted purge leaves the table describable.
	//! Like CatalogUtil.dropTableData, this deletes what the metadata references and nothing else, and the metadata-log
	//! is the only inventory of a table's previous metadata files. The AWS Glue Iceberg REST endpoint writes every
	//! metadata file with an empty metadata-log (in the file itself, not only in the LoadTable response), so for a
	//! Glue table the previous metadata files (0000N-*.metadata.json, one per commit) are unreachable from here and
	//! stay behind in the metadata directory after the drop. Reported to the Glue team.
	//! TODO: once the extension has a remove_orphan_files procedure (list the table location, delete every file no
	//! snapshot, statistics entry or metadata-log entry reaches), run it after the catalog accepted the drop to clean
	//! those up too. After the DELETE no commit can land anymore, so the age threshold remove_orphan_files normally
	//! needs against concurrent writers is not needed there.
	unordered_set<string> seen_metadata;
	for (auto &log_item : metadata.metadata_log) {
		AddUnique(files.metadata_files, seen_metadata, log_item.metadata_file);
	}
	if (metadata.metadata_location.empty()) {
		//! The catalog response carried no 'metadata-location' (it is optional in the REST spec), or the metadata was
		//! built without recording where it came from: the current metadata file cannot be deleted.
		DUCKDB_LOG_WARNING(context, "%s, the location of the current metadata file is unknown, it will be left behind",
		                   operation);
	}
	AddUnique(files.metadata_files, seen_metadata, metadata.metadata_location);
	return files;
}

void IcebergTablePurge::DeleteFiles(ClientContext &context, const IcebergTableFiles &files, const string &operation) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto remove = [&](const vector<string> &paths, const char *description) {
		IcebergFileCleanup::RemoveFilesBestEffort(context, fs, paths, description, operation.c_str());
	};
	if (files.data_files_retained) {
		DUCKDB_LOG(context, IcebergLogType, "%s, keeps the data files because the table has 'gc.enabled' = false",
		           operation);
	}
	remove(files.data_files, "data file");
	remove(files.manifests, "manifest");
	remove(files.manifest_lists, "manifest list");
	remove(files.statistics_files, "statistics file");
	remove(files.metadata_files, "metadata file");
}

} // namespace duckdb
