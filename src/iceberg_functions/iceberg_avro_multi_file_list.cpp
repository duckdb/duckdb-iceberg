#include "iceberg_avro_multi_file_list.hpp"
#include "metadata/iceberg_manifest.hpp"

namespace duckdb {

IcebergAvroScanInfo::IcebergAvroScanInfo(AvroScanInfoType type, const IcebergTableMetadata &metadata,
                                         const IcebergSnapshot &snapshot)
    : type(type), metadata(metadata), snapshot(snapshot) {
}
IcebergAvroScanInfo::~IcebergAvroScanInfo() {
}

IcebergManifestListScanInfo::IcebergManifestListScanInfo(const IcebergTableMetadata &metadata,
                                                         const IcebergSnapshot &snapshot)
    : IcebergAvroScanInfo(TYPE, metadata, snapshot) {
}
IcebergManifestListScanInfo::~IcebergManifestListScanInfo() {
}

IcebergManifestFileScanInfo::IcebergManifestFileScanInfo(const IcebergTableMetadata &metadata,
                                                         const IcebergSnapshot &snapshot,
                                                         const vector<IcebergManifestFile> &manifest_files,
                                                         const IcebergOptions &options, FileSystem &fs,
                                                         const string &iceberg_path)
    : IcebergAvroScanInfo(TYPE, metadata, snapshot), manifest_files(manifest_files), options(options), fs(fs),
      iceberg_path(iceberg_path) {
	unordered_set<int32_t> partition_spec_ids;
	for (auto &manifest_file : manifest_files) {
		partition_spec_ids.insert(manifest_file.partition_spec_id);
	}
	//! The schema of a manifest is affected by the 'partition_spec_id' of the 'manifest_file',
	//! because the 'partition' struct has a field for every partition field in that partition spec.

	//! Since we are now reading *all* manifests in one reader, we have to merge these schemas,
	//! and to do that we create a map of all relevant partition fields
	partition_field_id_to_type = IcebergDataFile::GetFieldIdToTypeMapping(snapshot, metadata, partition_spec_ids);
}

IcebergManifestFileScanInfo::~IcebergManifestFileScanInfo() {
}

const IcebergManifestFile &IcebergManifestFileScanInfo::GetManifestFile(idx_t file_idx) const {
	return manifest_files[file_idx];
}

IcebergAvroMultiFileList::IcebergAvroMultiFileList(shared_ptr<IcebergAvroScanInfo> info, vector<OpenFileInfo> paths)
    : SimpleMultiFileList(std::move(paths)), info(info) {
}
IcebergAvroMultiFileList::~IcebergAvroMultiFileList() {
}

} // namespace duckdb
