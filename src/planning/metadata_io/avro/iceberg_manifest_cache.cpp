#include "planning/metadata_io/avro/iceberg_manifest_cache.hpp"

#include "common/iceberg_utils.hpp"
#include "planning/metadata_io/avro/iceberg_avro_multi_file_list.hpp"

namespace duckdb {

namespace {

idx_t ValueMemory(const Value &value) {
	// Include the logical type/extra-info allocation, even for shared types.
	idx_t size = 64 + value.type().ToString().size();
	if (value.IsNull()) {
		return size;
	}
	if (value.type().InternalType() == PhysicalType::VARCHAR) {
		return size + sizeof(string) + StringValue::Get(value).capacity() + 1;
	}
	optional_ptr<const vector<Value>> children;
	switch (value.type().id()) {
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::UNION:
		children = &StructValue::GetChildren(value);
		break;
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		children = &ListValue::GetChildren(value);
		break;
	case LogicalTypeId::ARRAY:
		children = &ArrayValue::GetChildren(value);
		break;
	default:
		break;
	}
	if (children) {
		size += sizeof(vector<Value>) + children->capacity() * sizeof(Value);
		for (auto &child : *children) {
			size += ValueMemory(child);
		}
	}
	return size;
}

template <class T>
idx_t MapMemory(const unordered_map<int32_t, T> &map) {
	return map.bucket_count() * sizeof(void *) +
	       map.size() * (sizeof(typename unordered_map<int32_t, T>::value_type) + 3 * sizeof(void *));
}

void KeyPart(string &key, const string &part) {
	key += std::to_string(part.size()) + ":" + part;
}

} // namespace

idx_t EstimateManifestEntries(const vector<IcebergManifestEntry> &entries) {
	idx_t size = sizeof(entries) + entries.capacity() * sizeof(IcebergManifestEntry);
	for (auto &entry : entries) {
		auto &file = entry.data_file;
		size += file.file_path.capacity() + file.file_format.capacity() + 2;
		if (file.referenced_data_file) {
			size += file.referenced_data_file->capacity() + 1;
		}
		size += file.partition_info.capacity() * sizeof(IcebergPartitionInfo);
		for (auto &field : file.partition_info) {
			size += ValueMemory(field.value);
		}
		size += file.equality_ids.capacity() * sizeof(int32_t) + file.split_offsets.capacity() * sizeof(int64_t);
		size += MapMemory(file.column_sizes) + MapMemory(file.value_counts) + MapMemory(file.null_value_counts) +
		        MapMemory(file.nan_value_counts) + MapMemory(file.lower_bounds) + MapMemory(file.upper_bounds);
		for (auto &bound : file.lower_bounds) {
			size += ValueMemory(bound.second);
		}
		for (auto &bound : file.upper_bounds) {
			size += ValueMemory(bound.second);
		}
	}
	return size;
}

string ManifestCacheKey(const IcebergManifestFileScanInfo &scan, idx_t manifest_idx) {
	auto &file = scan.manifest_files[manifest_idx].GetFile();
	auto path = scan.options.allow_moved_paths
	                ? IcebergUtils::GetFullPath(scan.iceberg_path, file.manifest_path, scan.fs)
	                : file.manifest_path;
	path = scan.fs.ExpandPath(path);
	if (!FileSystem::IsRemoteFile(path) && !scan.fs.IsPathAbsolute(path)) {
		path = scan.fs.JoinPath(FileSystem::GetWorkingDirectory(), path);
	}
	string key;
	KeyPart(key, scan.file_secret.cache_scope);
	KeyPart(key, scan.metadata.table_uuid);
	KeyPart(key, scan.metadata.location);
	KeyPart(key, path);
	KeyPart(key, std::to_string(file.manifest_length));
	KeyPart(key, std::to_string(file.partition_spec_id));
	KeyPart(key, std::to_string(scan.metadata.iceberg_version));
	// Use the actual merged Avro target types, not a table-local numeric schema ID.
	for (auto &field : scan.partition_field_id_to_type) {
		KeyPart(key, std::to_string(field.first));
		KeyPart(key, field.second.ToString());
	}
	KeyPart(key, "specs");
	// All specs affect v1 header resolution and whether DATE-to-INTEGER day casts are enabled.
	map<int32_t, string> specs;
	for (auto &spec : scan.metadata.partition_specs) {
		specs.emplace(spec.first, spec.second.FieldsToJSONString());
	}
	for (auto &spec : specs) {
		KeyPart(key, std::to_string(spec.first));
		KeyPart(key, spec.second);
	}
	return key;
}

IcebergParsedManifest::IcebergParsedManifest(ClientContext &context, IcebergManifestListEntry &manifest,
                                             const IcebergTableMetadata &metadata, idx_t key_size)
    : entries(manifest.ShareManifestEntries()), header(manifest.manifest_header),
      estimated_memory(sizeof(*this) + 2 * key_size + 128) {
	estimated_memory += EstimateManifestEntries(*entries);
	// The ordered map has both a vector and a hash directory, with private capacities.
	// Conservatively allow 2x growth in both containers and two copies of each key.
	estimated_memory += header.size() * 4 * (sizeof(pair<string, Value>) + 4 * sizeof(void *));
	for (auto &field : header) {
		estimated_memory += 2 * (field.first.capacity() + 1) + ValueMemory(field.second);
	}
	if (manifest.manifest_metadata->content == IcebergManifestContentType::DELETE) {
		auto spec_id = manifest.manifest_metadata->partition_spec_id;
		auto spec = metadata.FindPartitionSpecById(spec_id);
		if (!spec) {
			throw InvalidConfigurationException("Manifest file '%s' key-value metadata references partition-spec-id: "
			                                    "%d, but no spec with that ID exists in the metadata",
			                                    manifest.GetFile().manifest_path, spec_id);
		}
		delete_index = make_shared_ptr<IcebergDeleteCandidateIndex>(context, entries, *spec);
		estimated_memory += delete_index->EstimatedIndexMemory();
	}
}

IcebergManifestMetadata IcebergParsedManifest::Validate(const IcebergManifestFileScanInfo &scan,
                                                        const IcebergManifestListEntry &manifest) const {
	auto &file = manifest.GetFile();
	auto metadata =
	    IcebergManifestMetadata::FromAvroMetadata(header, file.manifest_path, scan.metadata, file.partition_spec_id);
	if (metadata.content != file.content ||
	    (metadata.format_version >= 2 && metadata.partition_spec_id != file.partition_spec_id)) {
		throw InvalidConfigurationException("Manifest file '%s' header does not match its manifest-list descriptor",
		                                    file.manifest_path);
	}
	if (metadata.format_version > scan.metadata.iceberg_version) {
		throw InvalidConfigurationException(
		    "Manifest file has format-version %d, which is newer than the table's format-version %d",
		    metadata.format_version, scan.metadata.iceberg_version);
	}
	if (!scan.metadata.FindPartitionSpecById(metadata.partition_spec_id)) {
		throw InvalidConfigurationException("Manifest file '%s' key-value metadata references partition-spec-id: "
		                                    "%d, but no spec with that ID exists in the metadata",
		                                    file.manifest_path, metadata.partition_spec_id);
	}
	// Streaming misses enforce this before publishing references; hits must enforce it too.
	if (scan.read_state && file.counts && file.counts->FilesComplete()) {
		auto count =
		    *file.counts->added_files_count + *file.counts->existing_files_count + *file.counts->deleted_files_count;
		if (entries->size() > count) {
			throw InvalidConfigurationException("Manifest '%s' contains more entries than its manifest-list counts",
			                                    file.manifest_path);
		}
	}
	return metadata;
}

const vector<IcebergManifestEntry> &IcebergManifestListEntry::GetManifestEntries() const {
	if (parsed_manifest) {
		return parsed_manifest->Entries();
	}
	if (shared_entries) {
		return *shared_entries;
	}
	D_ASSERT(manifest_entries);
	return *manifest_entries;
}

vector<IcebergManifestEntry> &IcebergManifestListEntry::GetOrCreateManifestEntries() {
	if (parsed_manifest) {
		manifest_entries = parsed_manifest->Entries();
		parsed_manifest.reset();
		shared_entries.reset();
	} else if (shared_entries) {
		manifest_entries = *shared_entries;
		shared_entries.reset();
	}
	if (!manifest_entries) {
		manifest_entries.emplace();
	}
	return *manifest_entries;
}

vector<IcebergManifestEntry> IcebergManifestListEntry::TakeManifestEntries() {
	auto result = std::move(GetOrCreateManifestEntries());
	manifest_entries.reset();
	return result;
}

shared_ptr<const vector<IcebergManifestEntry>> IcebergManifestListEntry::ShareManifestEntries() {
	if (parsed_manifest) {
		return parsed_manifest->SharedEntries();
	}
	if (!shared_entries) {
		// Allocate the owner before moving. If allocation, index construction or
		// validation throws, this descriptor still owns every streamed entry.
		auto &entries = GetOrCreateManifestEntries();
		shared_entries = make_shared_ptr<const vector<IcebergManifestEntry>>(std::move(entries));
		manifest_entries.reset();
	}
	return shared_entries;
}

void IcebergManifestListEntry::ResetManifestEntries() {
	manifest_entries.reset();
	shared_entries.reset();
	parsed_manifest.reset();
	manifest_header.clear();
	manifest_metadata.reset();
}

} // namespace duckdb
