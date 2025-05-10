#include "duckdb.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_manifest.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_types.hpp"
#include "manifest_reader.hpp"
#include "catalog_utils.hpp"

namespace duckdb {

IcebergTable IcebergTable::Load(const string &iceberg_path, shared_ptr<IcebergSnapshot> snapshot,
                                ClientContext &context, const IcebergOptions &options) {
	IcebergTable ret;
	ret.path = iceberg_path;
	ret.snapshot = snapshot;

	vector<IcebergManifest> manifests;
	unique_ptr<ManifestReader> manifest_reader;
	unique_ptr<ManifestReader> manifest_entry_reader;
	manifest_reader_manifest_producer manifest_producer = nullptr;
	manifest_reader_manifest_entry_producer entry_producer = nullptr;

	//! Set up the manifest + manifest entry readers
	if (snapshot->iceberg_format_version == 1) {
		manifest_entry_reader = make_uniq<ManifestReader>(IcebergManifestEntryV1::PopulateNameMapping,
		                                                  IcebergManifestEntryV1::VerifySchema);
		manifest_reader =
		    make_uniq<ManifestReader>(IcebergManifestV1::PopulateNameMapping, IcebergManifestV1::VerifySchema);

		manifest_producer = IcebergManifestV1::ProduceEntries;
		entry_producer = IcebergManifestEntryV1::ProduceEntries;
	} else if (snapshot->iceberg_format_version == 2) {
		manifest_entry_reader = make_uniq<ManifestReader>(IcebergManifestEntryV2::PopulateNameMapping,
		                                                  IcebergManifestEntryV2::VerifySchema);
		manifest_reader =
		    make_uniq<ManifestReader>(IcebergManifestV2::PopulateNameMapping, IcebergManifestV2::VerifySchema);

		manifest_producer = IcebergManifestV2::ProduceEntries;
		entry_producer = IcebergManifestEntryV2::ProduceEntries;
	} else {
		throw InvalidInputException("Reading from Iceberg version %d is not supported yet",
		                            snapshot->iceberg_format_version);
	}
	auto &fs = FileSystem::GetFileSystem(context);
	auto manifest_list_full_path = options.allow_moved_paths
	                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot->manifest_list, fs)
	                                   : snapshot->manifest_list;
	auto scan = make_uniq<AvroScan>("IcebergManifestList", context, manifest_list_full_path);
	manifest_reader->Initialize(std::move(scan));

	vector<IcebergManifest> all_manifests;
	while (!manifest_reader->Finished()) {
		manifest_reader->ReadEntries(STANDARD_VECTOR_SIZE,
		                             [&all_manifests, manifest_producer](DataChunk &chunk, idx_t offset, idx_t count,
		                                                                 const ManifestReaderInput &input) {
			                             return manifest_producer(chunk, offset, count, input, all_manifests);
		                             });
	}

	for (auto &manifest : all_manifests) {
		auto manifest_entry_full_path = options.allow_moved_paths
		                                    ? IcebergUtils::GetFullPath(iceberg_path, manifest.manifest_path, fs)
		                                    : manifest.manifest_path;
		auto scan = make_uniq<AvroScan>("IcebergManifest", context, manifest_entry_full_path);
		manifest_entry_reader->Initialize(std::move(scan));
		manifest_entry_reader->SetSequenceNumber(manifest.sequence_number);
		manifest_entry_reader->SetPartitionSpecID(manifest.partition_spec_id);

		vector<IcebergManifestEntry> data_files;
		while (!manifest_entry_reader->Finished()) {
			manifest_entry_reader->ReadEntries(
			    STANDARD_VECTOR_SIZE, [&data_files, &entry_producer](DataChunk &chunk, idx_t offset, idx_t count,
			                                                         const ManifestReaderInput &input) {
				    return entry_producer(chunk, offset, count, input, data_files);
			    });
		}
		IcebergTableEntry table_entry;
		table_entry.manifest = std::move(manifest);
		table_entry.manifest_entries = std::move(data_files);
		ret.entries.push_back(table_entry);
	}
	return ret;
}

IcebergPartitionSpecField IcebergPartitionSpecField::ParseFromJson(yyjson_val *field) {
	IcebergPartitionSpecField result;

	result.name = IcebergUtils::TryGetStrFromObject(field, "name");
	result.transform = IcebergTransform(IcebergUtils::TryGetStrFromObject(field, "transform"));
	result.source_id = IcebergUtils::TryGetNumFromObject(field, "source-id");
	result.partition_field_id = IcebergUtils::TryGetNumFromObject(field, "field-id");
	return result;
}

IcebergPartitionSpec IcebergPartitionSpec::ParseFromJson(yyjson_val *partition_spec) {
	IcebergPartitionSpec result;
	result.spec_id = IcebergUtils::TryGetNumFromObject(partition_spec, "spec-id");
	auto fields = yyjson_obj_getn(partition_spec, "fields", strlen("fields"));
	if (!fields || yyjson_get_type(fields) != YYJSON_TYPE_ARR) {
		throw IOException("Invalid field found while parsing field: 'fields'");
	}

	size_t idx, max;
	yyjson_val *field;
	yyjson_arr_foreach(fields, idx, max, field) {
		result.fields.push_back(IcebergPartitionSpecField::ParseFromJson(field));
	}
	return result;
}

bool IcebergPartitionSpec::IsPartitioned() const {
	//! A partition spec is considered partitioned if it has at least one field that doesn't have a 'void' transform
	for (const auto &field : fields) {
		if (field.transform != IcebergTransformType::VOID) {
			return true;
		}
	}

	return false;
}

bool IcebergPartitionSpec::IsUnpartitioned() const {
	return !IsPartitioned();
}

const IcebergPartitionSpecField &IcebergPartitionSpec::GetFieldBySourceId(idx_t source_id) const {
	for (auto &field : fields) {
		if (field.source_id == source_id) {
			return field;
		}
	}
	throw InvalidConfigurationException("Field with source_id %d doesn't exist in this partition spec (id %d)",
	                                    source_id, spec_id);
}

static void ParseFieldMappings(yyjson_val *obj, vector<IcebergFieldMapping> &mappings, idx_t &mapping_index,
                               idx_t parent_mapping_index) {
	case_insensitive_map_t<IcebergFieldMapping> result;
	size_t idx, max;
	yyjson_val *val;
	yyjson_arr_foreach(obj, idx, max, val) {
		auto names = yyjson_obj_get(val, "names");
		auto field_id = yyjson_obj_get(val, "field-id");
		auto fields = yyjson_obj_get(val, "fields");

		//! Create a new mapping entry
		mappings.emplace_back();
		auto &mapping = mappings.back();

		if (!names) {
			throw InvalidInputException("Corrupt metadata.json file, field-mapping is missing names!");
		}
		auto current_mapping_index = mapping_index;

		auto &name_to_mapping_index = mappings[parent_mapping_index].field_mapping_indexes;
		//! Map every entry in the 'names' list to the entry we created above
		size_t names_idx, names_max;
		yyjson_val *names_val;
		yyjson_arr_foreach(names, names_idx, names_max, names_val) {
			auto name = yyjson_get_str(names_val);
			name_to_mapping_index[name] = current_mapping_index;
		}
		mapping_index++;

		if (field_id) {
			mapping.field_id = yyjson_get_sint(field_id);
		}
		//! Create mappings for the the nested fields
		if (fields) {
			ParseFieldMappings(fields, mappings, mapping_index, current_mapping_index);
		}
	}
}

static unordered_map<int64_t, IcebergPartitionSpec> ParsePartitionSpecs(yyjson_val *partition_specs) {
	unordered_map<int64_t, IcebergPartitionSpec> result;

	size_t idx, max;
	yyjson_val *partition_spec;
	yyjson_arr_foreach(partition_specs, idx, max, partition_spec) {
		auto spec = IcebergPartitionSpec::ParseFromJson(partition_spec);
		result[spec.spec_id] = spec;
	}
	return result;
}

unique_ptr<IcebergMetadata> IcebergMetadata::Parse(const string &path, FileSystem &fs,
                                                   const string &metadata_compression_codec) {
	auto metadata = unique_ptr<IcebergMetadata>(new IcebergMetadata);

	if (metadata_compression_codec == "gzip" || StringUtil::EndsWith(path, "gz.metadata.json")) {
		metadata->document = IcebergUtils::GzFileToString(path, fs);
	} else {
		metadata->document = IcebergUtils::FileToString(path, fs);
	}
	metadata->doc = yyjson_read(metadata->document.c_str(), metadata->document.size(), 0);
	if (metadata->doc == nullptr) {
		throw InvalidInputException("Fails to parse iceberg metadata from %s", path);
	}

	auto &info = *metadata;
	auto root = yyjson_doc_get_root(info.doc);
	info.iceberg_version = IcebergUtils::TryGetNumFromObject(root, "format-version");
	info.snapshots = yyjson_obj_get(root, "snapshots");
	auto partition_specs = yyjson_obj_get(root, "partition-specs");
	if (!partition_specs) {
		throw NotImplementedException("Support for V1 'partition-spec' missing");
	}
	info.partition_specs = ParsePartitionSpecs(partition_specs);

	// Multiple schemas can be present in the json metadata 'schemas' list
	if (yyjson_obj_getn(root, "current-schema-id", string("current-schema-id").size())) {
		size_t idx, max;
		yyjson_val *schema;
		info.schema_id = IcebergUtils::TryGetNumFromObject(root, "current-schema-id");
		auto schemas = yyjson_obj_get(root, "schemas");
		yyjson_arr_foreach(schemas, idx, max, schema) {
			info.schemas.push_back(schema);
		}
	} else {
		auto schema = yyjson_obj_get(root, "schema");
		if (!schema) {
			throw InvalidConfigurationException("Neither a valid schema or schemas field was found");
		}
		auto found_schema_id = IcebergUtils::TryGetNumFromObject(schema, "schema-id");
		info.schemas.push_back(schema);
		info.schema_id = found_schema_id;
	}

	auto properties = yyjson_obj_get(root, "properties");
	if (properties) {
		auto name_mapping_defaults_p = yyjson_obj_get(properties, "schema.name-mapping.default");
		if (name_mapping_defaults_p) {
			string name_mapping_default = yyjson_get_str(name_mapping_defaults_p);
			auto doc = std::unique_ptr<yyjson_doc, YyjsonDocDeleter>(
			    yyjson_read(name_mapping_default.c_str(), name_mapping_default.size(), 0));
			if (doc == nullptr) {
				throw InvalidInputException(
				    "Fails to parse iceberg metadata 'schema.name-mapping.default' property from %s", path);
			}
			auto root = yyjson_doc_get_root(doc.get());
			idx_t mapping_index = 0;
			metadata->mappings.emplace_back();
			mapping_index++;
			ParseFieldMappings(root, metadata->mappings, mapping_index, 0);
		}
	}
	return metadata;
}

shared_ptr<IcebergSnapshot> IcebergSnapshot::GetLatestSnapshot(IcebergMetadata &info, const IcebergOptions &options) {
	auto latest_snapshot = FindLatestSnapshotInternal(info.snapshots);
	return ParseSnapShot(latest_snapshot, info, options);
}

shared_ptr<IcebergSnapshot> IcebergSnapshot::GetSnapshotById(IcebergMetadata &info, idx_t snapshot_id,
                                                             const IcebergOptions &options) {
	auto snapshot = FindSnapshotByIdInternal(info.snapshots, snapshot_id);

	if (!snapshot) {
		throw InvalidConfigurationException("Could not find snapshot with id " + to_string(snapshot_id));
	}

	return ParseSnapShot(snapshot, info, options);
}

shared_ptr<IcebergSnapshot> IcebergSnapshot::GetSnapshotByTimestamp(IcebergMetadata &info, timestamp_t timestamp,
                                                                    const IcebergOptions &options) {
	auto snapshot = FindSnapshotByIdTimestampInternal(info.snapshots, timestamp);

	if (!snapshot) {
		throw InvalidConfigurationException("Could not find latest snapshots for timestamp " +
		                                    Timestamp::ToString(timestamp));
	}

	return ParseSnapShot(snapshot, info, options);
}

// Function to generate a metadata file url from version and format string
// default format is "v%s%s.metadata.json" -> v00###-xxxxxxxxx-.gz.metadata.json"
static string GenerateMetaDataUrl(FileSystem &fs, const string &meta_path, string &table_version,
                                  const IcebergOptions &options) {
	// TODO: Need to URL Encode table_version
	string compression_suffix = "";
	string url;
	if (options.metadata_compression_codec == "gzip") {
		compression_suffix = ".gz";
	}
	for (auto try_format : StringUtil::Split(options.version_name_format, ',')) {
		url = fs.JoinPath(meta_path, StringUtil::Format(try_format, table_version, compression_suffix));
		if (fs.FileExists(url)) {
			return url;
		}
	}

	throw InvalidConfigurationException(
	    "Iceberg metadata file not found for table version '%s' using '%s' compression and format(s): '%s'",
	    table_version, options.metadata_compression_codec, options.version_name_format);
}

string IcebergSnapshot::GetMetaDataPath(ClientContext &context, const string &path, FileSystem &fs,
                                        const IcebergOptions &options) {
	string version_hint;
	string meta_path = fs.JoinPath(path, "metadata");

	auto &table_version = options.table_version;

	if (StringUtil::EndsWith(path, ".json")) {
		// We've been given a real metadata path. Nothing else to do.
		return path;
	}
	if (StringUtil::EndsWith(table_version, ".text") || StringUtil::EndsWith(table_version, ".txt")) {
		// We were given a hint filename
		version_hint = GetTableVersionFromHint(meta_path, fs, table_version);
		return GenerateMetaDataUrl(fs, meta_path, version_hint, options);
	}
	if (table_version != UNKNOWN_TABLE_VERSION) {
		// We were given an explicit version number
		version_hint = table_version;
		return GenerateMetaDataUrl(fs, meta_path, version_hint, options);
	}
	if (fs.FileExists(fs.JoinPath(meta_path, DEFAULT_VERSION_HINT_FILE))) {
		// We're guessing, but a version-hint.text exists so we'll use that
		version_hint = GetTableVersionFromHint(meta_path, fs, DEFAULT_VERSION_HINT_FILE);
		return GenerateMetaDataUrl(fs, meta_path, version_hint, options);
	}
	if (!UnsafeVersionGuessingEnabled(context)) {
		// Make sure we're allowed to guess versions
		throw InvalidConfigurationException(
		    "Failed to read iceberg table. No version was provided and no version-hint could be found, globbing the "
		    "filesystem to locate the latest version is disabled by default as this is considered unsafe and could "
		    "result in reading uncommitted data. To enable this use 'SET %s = true;'",
		    VERSION_GUESSING_CONFIG_VARIABLE);
	}

	// We are allowed to guess to guess from file paths
	return GuessTableVersion(meta_path, fs, options);
}

shared_ptr<IcebergSnapshot> IcebergSnapshot::ParseSnapShot(yyjson_val *snapshot, IcebergMetadata &metadata,
                                                           const IcebergOptions &options) {
	auto ret = make_shared_ptr<IcebergSnapshot>();
	if (snapshot) {
		auto snapshot_tag = yyjson_get_type(snapshot);
		if (snapshot_tag != YYJSON_TYPE_OBJ) {
			throw InvalidConfigurationException("Invalid snapshot field found parsing iceberg metadata.json");
		}
		ret->metadata_compression_codec = options.metadata_compression_codec;
		if (metadata.iceberg_version == 1) {
			ret->sequence_number = 0;
		} else if (metadata.iceberg_version == 2) {
			ret->sequence_number = IcebergUtils::TryGetNumFromObject(snapshot, "sequence-number");
		}
		ret->snapshot_id = IcebergUtils::TryGetNumFromObject(snapshot, "snapshot-id");
		ret->timestamp_ms = Timestamp::FromEpochMs(IcebergUtils::TryGetNumFromObject(snapshot, "timestamp-ms"));
		auto schema_id = yyjson_obj_get(snapshot, "schema-id");
		if (options.snapshot_source == SnapshotSource::LATEST) {
			ret->schema_id = metadata.schema_id;
		} else if (schema_id && yyjson_get_type(schema_id) == YYJSON_TYPE_NUM) {
			ret->schema_id = yyjson_get_uint(schema_id);
		} else {
			//! 'schema-id' is optional in the V1 iceberg format.
			D_ASSERT(metadata.iceberg_version == 1);
			ret->schema_id = metadata.schema_id;
		}
		ret->manifest_list = IcebergUtils::TryGetStrFromObject(snapshot, "manifest-list");
	} else {
		ret->snapshot_id = DConstants::INVALID_INDEX;
		ret->schema_id = metadata.schema_id;
	}

	ret->iceberg_format_version = metadata.iceberg_version;
	if (options.infer_schema) {
		ret->schema = ParseSchema(metadata.schemas, ret->schema_id);
	}
	return ret;
}

string IcebergSnapshot::GetTableVersionFromHint(const string &meta_path, FileSystem &fs,
                                                string version_file = DEFAULT_VERSION_HINT_FILE) {
	auto version_file_path = fs.JoinPath(meta_path, version_file);
	auto version_file_content = IcebergUtils::FileToString(version_file_path, fs);

	try {
		return version_file_content;
	} catch (std::invalid_argument &e) {
		throw InvalidConfigurationException("Iceberg version hint file contains invalid value");
	} catch (std::out_of_range &e) {
		throw InvalidConfigurationException("Iceberg version hint file contains invalid value");
	}
}

bool IcebergSnapshot::UnsafeVersionGuessingEnabled(ClientContext &context) {
	Value result;
	(void)context.TryGetCurrentSetting(VERSION_GUESSING_CONFIG_VARIABLE, result);
	return !result.IsNull() && result.GetValue<bool>();
}

string IcebergSnapshot::GuessTableVersion(const string &meta_path, FileSystem &fs, const IcebergOptions &options) {
	string selected_metadata;
	string version_pattern = "*"; // TODO: Different "table_version" strings could customize this
	string compression_suffix = "";

	auto &metadata_compression_codec = options.metadata_compression_codec;
	auto &version_format = options.version_name_format;

	if (metadata_compression_codec == "gzip") {
		compression_suffix = ".gz";
	}

	for (auto try_format : StringUtil::Split(version_format, ',')) {
		auto glob_pattern = StringUtil::Format(try_format, version_pattern, compression_suffix);

		auto found_versions = fs.Glob(fs.JoinPath(meta_path, glob_pattern));
		if (found_versions.size() > 0) {
			selected_metadata = PickTableVersion(found_versions, version_pattern, glob_pattern);
			if (!selected_metadata.empty()) { // Found one
				return selected_metadata;
			}
		}
	}

	throw InvalidConfigurationException(
	    "Could not guess Iceberg table version using '%s' compression and format(s): '%s'", metadata_compression_codec,
	    version_format);
}

string IcebergSnapshot::PickTableVersion(vector<OpenFileInfo> &found_metadata, string &version_pattern, string &glob) {
	// TODO: Different "table_version" strings could customize this
	// For now: just sort the versions and take the largest
	if (!found_metadata.empty()) {
		std::sort(found_metadata.begin(), found_metadata.end(),
		          [](const OpenFileInfo &a, const OpenFileInfo &b) { return a.path < b.path; });
		return found_metadata.back().path;
	} else {
		return string();
	}
}

yyjson_val *IcebergSnapshot::FindLatestSnapshotInternal(yyjson_val *snapshots) {
	size_t idx, max;
	yyjson_val *snapshot;

	uint64_t max_timestamp = NumericLimits<uint64_t>::Minimum();
	yyjson_val *max_snapshot = nullptr;

	yyjson_arr_foreach(snapshots, idx, max, snapshot) {

		auto timestamp = IcebergUtils::TryGetNumFromObject(snapshot, "timestamp-ms");
		if (timestamp >= max_timestamp) {
			max_timestamp = timestamp;
			max_snapshot = snapshot;
		}
	}

	return max_snapshot;
}

yyjson_val *IcebergSnapshot::FindSnapshotByIdInternal(yyjson_val *snapshots, idx_t target_id) {
	size_t idx, max;
	yyjson_val *snapshot;

	yyjson_arr_foreach(snapshots, idx, max, snapshot) {

		auto snapshot_id = IcebergUtils::TryGetNumFromObject(snapshot, "snapshot-id");

		if (snapshot_id == target_id) {
			return snapshot;
		}
	}

	return nullptr;
}

yyjson_val *IcebergSnapshot::IcebergSnapshot::FindSnapshotByIdTimestampInternal(yyjson_val *snapshots,
                                                                                timestamp_t timestamp) {
	size_t idx, max;
	yyjson_val *snapshot;

	uint64_t max_millis = NumericLimits<uint64_t>::Minimum();
	yyjson_val *max_snapshot = nullptr;

	auto timestamp_millis = Timestamp::GetEpochMs(timestamp);

	yyjson_arr_foreach(snapshots, idx, max, snapshot) {
		auto curr_millis = IcebergUtils::TryGetNumFromObject(snapshot, "timestamp-ms");

		if (curr_millis <= timestamp_millis && curr_millis >= max_millis) {
			max_snapshot = snapshot;
			max_millis = curr_millis;
		}
	}

	return max_snapshot;
}

} // namespace duckdb
