#include "planning/metadata_io/avro/iceberg_avro_multi_file_list.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"

namespace duckdb {

IcebergAvroScanInfo::IcebergAvroScanInfo(AvroScanInfoType type, const IcebergTableMetadata &metadata,
                                         const IcebergSnapshotScanInfo &snapshot_info)
    : type(type), metadata(metadata), snapshot_info(snapshot_info) {
}
IcebergAvroScanInfo::~IcebergAvroScanInfo() {
}

IcebergManifestListScanInfo::IcebergManifestListScanInfo(const IcebergTableMetadata &metadata,
                                                         const IcebergSnapshotScanInfo &snapshot_info,
                                                         vector<IcebergManifestListEntry> &result)
    : IcebergAvroScanInfo(TYPE, metadata, snapshot_info), result(result) {
}
IcebergManifestListScanInfo::~IcebergManifestListScanInfo() {
}

IcebergManifestFileScanInfo::IcebergManifestFileScanInfo(
    const IcebergTableMetadata &metadata, const IcebergSnapshotScanInfo &snapshot_info,
    vector<IcebergManifestListEntry> &manifest_files, const IcebergOptions &options, FileSystem &fs,
    const string &iceberg_path, optional_ptr<ManifestEntryReadState> read_state, vector<idx_t> manifest_indexes_p)
    : IcebergAvroScanInfo(TYPE, metadata, snapshot_info), manifest_files(manifest_files), options(options), fs(fs),
      iceberg_path(iceberg_path), read_state(read_state) {
	if (manifest_indexes_p.empty()) {
		manifest_indexes_p.reserve(manifest_files.size());
		for (idx_t manifest_idx = 0; manifest_idx < manifest_files.size(); manifest_idx++) {
			manifest_indexes_p.push_back(manifest_idx);
		}
	}
	manifest_indexes = std::move(manifest_indexes_p);
	unordered_set<int32_t> partition_spec_ids;
	for (auto manifest_idx : manifest_indexes) {
		if (manifest_idx >= manifest_files.size()) {
			throw InternalException("Manifest selection index %llu is out of bounds", manifest_idx);
		}
		auto &manifest_list_entry = manifest_files[manifest_idx];
		auto &manifest = manifest_list_entry.file;
		partition_spec_ids.insert(manifest.partition_spec_id);
	}
	//! The schema of a manifest is affected by the 'partition_spec_id' of the 'manifest_file',
	//! because the 'partition' struct has a field for every partition field in that partition spec.

	//! Since we are now reading *all* manifests in one reader, we have to merge these schemas,
	//! and to do that we create a map of all relevant partition fields
	partition_field_id_to_type = IcebergDataFile::GetFieldIdToTypeMapping(snapshot_info, metadata, partition_spec_ids);
}

IcebergManifestFileScanInfo::~IcebergManifestFileScanInfo() {
}

idx_t IcebergManifestFileScanInfo::GetManifestIndex(idx_t scan_index) const {
	if (scan_index >= manifest_indexes.size()) {
		throw InternalException("Manifest scan index %llu is out of bounds", scan_index);
	}
	return manifest_indexes[scan_index];
}

IcebergAvroMultiFileList::IcebergAvroMultiFileList(shared_ptr<IcebergAvroScanInfo> info, vector<OpenFileInfo> paths)
    : SimpleMultiFileList(std::move(paths)), info(info) {
}
IcebergAvroMultiFileList::~IcebergAvroMultiFileList() {
}

} // namespace duckdb
