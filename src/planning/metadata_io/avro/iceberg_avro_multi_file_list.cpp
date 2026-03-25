#include "planning/metadata_io/avro/iceberg_avro_multi_file_list.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"

namespace duckdb {

IcebergAvroScanInfo::IcebergAvroScanInfo(AvroScanInfoType type, const IcebergTableMetadata &metadata,
                                         const IcebergSnapshot &snapshot)
    : type(type), metadata(metadata), snapshot(snapshot) {
}
IcebergAvroScanInfo::~IcebergAvroScanInfo() {
}

IcebergManifestListScanInfo::IcebergManifestListScanInfo(const IcebergTableMetadata &metadata,
                                                         const IcebergSnapshot &snapshot,
                                                         vector<IcebergManifestListEntry> &result)
    : IcebergAvroScanInfo(TYPE, metadata, snapshot), result(result) {
}
IcebergManifestListScanInfo::~IcebergManifestListScanInfo() {
}

IcebergManifestFileScanInfo::IcebergManifestFileScanInfo(const IcebergTableMetadata &metadata,
                                                         const IcebergSnapshot &snapshot,
                                                         vector<IcebergManifestListEntry> &manifest_files,
                                                         const IcebergOptions &options, FileSystem &fs,
                                                         const string &iceberg_path,
                                                         optional_ptr<ManifestEntryReadState> read_state_p)
    : IcebergAvroScanInfo(TYPE, metadata, snapshot), options(options), fs(fs), iceberg_path(iceberg_path),
      read_state(read_state_p) {
	unordered_set<int32_t> partition_spec_ids;
	for (idx_t i = 0; i < manifest_files.size(); i++) {
		auto &manifest_list_entry = manifest_files[i];
		auto &manifest = manifest_list_entry.GetManifest();
		if (manifest.CanCache()) {
			if (read_state_p) {
				//! Push a batch for this manifest directly
				auto &read_state = *read_state_p;
				read_state.PushBatch(ManifestReadBatch(i, 0, manifest.manifest_entries.size()));
			}
			continue;
		}

		files_to_scan.emplace_back(i, manifest_list_entry);
		auto &manifest_file = manifest_list_entry.ManifestFile();
		partition_spec_ids.insert(manifest_file.partition_spec_id);
	}
	//! The schema of a manifest is affected by the 'partition_spec_id' of the 'manifest_file',
	//! because the 'partition' struct has a field for every partition field in that partition spec.

	//! Since we are now reading *all* manifests in one reader, we have to merge these schemas,
	//! and to do that we create a map of all relevant partition fields
	if (!partition_spec_ids.empty()) {
		partition_field_id_to_type = IcebergDataFile::GetFieldIdToTypeMapping(snapshot, metadata, partition_spec_ids);
	}
}

IcebergManifestFileScanInfo::~IcebergManifestFileScanInfo() {
}

IcebergAvroMultiFileList::IcebergAvroMultiFileList(shared_ptr<IcebergAvroScanInfo> info, vector<OpenFileInfo> paths)
    : SimpleMultiFileList(std::move(paths)), info(info) {
}
IcebergAvroMultiFileList::~IcebergAvroMultiFileList() {
}

} // namespace duckdb
