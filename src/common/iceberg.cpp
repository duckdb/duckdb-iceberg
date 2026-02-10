#include "duckdb.hpp"
#include "iceberg_metadata.hpp"
#include "avro_scan.hpp"
#include "iceberg_utils.hpp"
#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "manifest_reader.hpp"
#include "catalog_utils.hpp"

namespace duckdb {

IcebergTable::IcebergTable(const IcebergSnapshot &snapshot_p) : snapshot(snapshot_p) {
}

unique_ptr<IcebergTable> IcebergTable::Load(const string &iceberg_path, const IcebergTableMetadata &metadata,
                                            const IcebergSnapshot &snapshot, ClientContext &context,
                                            const IcebergOptions &options) {
	auto ret = make_uniq<IcebergTable>(snapshot);
	ret->path = iceberg_path;

	auto &fs = FileSystem::GetFileSystem(context);
	auto manifest_list_full_path = options.allow_moved_paths
	                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
	                                   : snapshot.manifest_list;
	IcebergManifestList manifest_list(manifest_list_full_path);

	//! Read the entire manifest list, producing 'manifest_file' items
	auto manifest_list_reader = make_uniq<manifest_list::ManifestListReader>(metadata.iceberg_version);
	auto scan = AvroScan::ScanManifestList(snapshot, metadata, context, manifest_list_full_path);
	manifest_list_reader->Initialize(std::move(scan));

	vector<IcebergManifestFile> manifest_files;
	while (!manifest_list_reader->Finished()) {
		manifest_list_reader->Read(STANDARD_VECTOR_SIZE, manifest_files);
	}

	//! Read all manifest files, producing 'manifest_entry' items
	auto manifest_file_reader = make_uniq<manifest_file::ManifestReader>(metadata.iceberg_version, false);
	auto manifest_scan = AvroScan::ScanManifest(snapshot, manifest_files, options, fs, iceberg_path, metadata, context);
	manifest_file_reader->Initialize(std::move(manifest_scan));

	vector<IcebergManifestEntry> manifest_entries;
	while (!manifest_file_reader->Finished()) {
		manifest_file_reader->Read(STANDARD_VECTOR_SIZE, manifest_entries);
	}

	//! Create the table entries, then populate them
	for (auto &manifest_file : manifest_files) {
		ret->entries.push_back(std::move(manifest_file));
	}

	auto &entries = ret->entries;
	for (auto &manifest_entry : manifest_entries) {
		auto &manifest = entries[manifest_entry.manifest_file_idx];
		manifest.manifest_file.entries.push_back(std::move(manifest_entry));
	}
	return ret;
}

} // namespace duckdb
