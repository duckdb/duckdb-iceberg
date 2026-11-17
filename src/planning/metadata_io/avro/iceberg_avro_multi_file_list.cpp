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
	manifest_locks.reserve(manifest_files.size());
	for (idx_t manifest_idx = 0; manifest_idx < manifest_files.size(); manifest_idx++) {
		manifest_locks.push_back(make_uniq<mutex>());
	}
	unordered_set<int32_t> partition_spec_ids;
	for (auto manifest_idx : manifest_indexes) {
		if (manifest_idx >= manifest_files.size()) {
			throw InternalException("Manifest selection index %llu is out of bounds", manifest_idx);
		}
		auto &manifest_list_entry = manifest_files[manifest_idx];
		auto &manifest = manifest_list_entry.GetFile();
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

mutex &IcebergManifestFileScanInfo::GetManifestLock(idx_t manifest_index) {
	if (manifest_index >= manifest_locks.size()) {
		throw InternalException("Manifest lock index %llu is out of bounds", manifest_index);
	}
	return *manifest_locks[manifest_index];
}

void IcebergManifestFileScanInfo::PublishEntries(idx_t manifest_index, vector<IcebergManifestEntry> &entries) {
	auto &manifest_file = manifest_files[manifest_index];
	lock_guard<mutex> manifest_guard(GetManifestLock(manifest_index));
	auto &manifest_entries = manifest_file.GetOrCreateManifestEntries();
	idx_t start_index = manifest_entries.size();
	if (read_state) {
		//! Streaming consumers retain references into this vector after publication. AvroScan
		//! reserves the exact manifest-list file count for selected misses, so exceeding the
		//! reserved capacity means the advertised count was wrong; appending would invalidate those
		//! references.
		if (entries.size() > manifest_entries.capacity() - manifest_entries.size()) {
			throw InvalidConfigurationException("Manifest '%s' contains more entries than its manifest-list counts",
			                                    manifest_file.GetFile().manifest_path);
		}
	}
	for (auto &entry : entries) {
		manifest_entries.push_back(std::move(entry));
	}
	if (read_state) {
		read_state->PushBatch(ManifestReadBatch(manifest_index, start_index, manifest_entries.size()));
	}
}

void IcebergManifestFileScanInfo::AddEntries(idx_t batch_index, idx_t manifest_index,
                                             vector<IcebergManifestEntry> entries) {
	lock_guard<mutex> guard(batch_lock);
	if (batch_index < next_batch_index) {
		throw InternalException("Manifest block %llu was already added", batch_index);
	}
	if (batch_index == next_batch_index) {
		PublishEntries(manifest_index, entries);
		return;
	}
	auto &pending = pending_batches[batch_index];
	pending.manifest_index = manifest_index;
	for (auto &entry : entries) {
		pending.entries.push_back(std::move(entry));
	}
}

void IcebergManifestFileScanInfo::FinishBatch(idx_t batch_index) {
	lock_guard<mutex> guard(batch_lock);
	if (batch_index < next_batch_index) {
		//! Already added by FlushBatches
		return;
	}
	finished_batches.insert(batch_index);
	while (true) {
		auto pending = pending_batches.find(next_batch_index);
		if (pending != pending_batches.end()) {
			PublishEntries(pending->second.manifest_index, pending->second.entries);
			pending_batches.erase(pending);
		}
		auto finished = finished_batches.find(next_batch_index);
		if (finished == finished_batches.end()) {
			break;
		}
		finished_batches.erase(finished);
		next_batch_index++;
	}
}

void IcebergManifestFileScanInfo::FlushBatches() {
	lock_guard<mutex> guard(batch_lock);
	//! A block without entries never finishes, so the blocks after it are still pending
	for (auto &pending : pending_batches) {
		PublishEntries(pending.second.manifest_index, pending.second.entries);
		next_batch_index = pending.first + 1;
	}
	pending_batches.clear();
	finished_batches.clear();
}

IcebergAvroMultiFileList::IcebergAvroMultiFileList(shared_ptr<IcebergAvroScanInfo> info, vector<OpenFileInfo> paths)
    : SimpleMultiFileList(std::move(paths)), info(info) {
}
IcebergAvroMultiFileList::~IcebergAvroMultiFileList() {
}

} // namespace duckdb
