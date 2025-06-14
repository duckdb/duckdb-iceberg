#include "metadata/iceberg_table_metadata.hpp"

#include "iceberg_utils.hpp"
#include "catalog_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {

//! ----------- Select Snapshot -----------

optional_ptr<IcebergSnapshot> IcebergTableMetadata::FindSnapshotByIdInternal(int64_t target_id) {
	auto it = snapshots.find(target_id);
	if (it == snapshots.end()) {
		return nullptr;
	}
	return it->second;
}

optional_ptr<IcebergSnapshot> IcebergTableMetadata::FindSnapshotByIdTimestampInternal(timestamp_t timestamp) {
	uint64_t max_millis = NumericLimits<uint64_t>::Minimum();
	optional_ptr<IcebergSnapshot> max_snapshot = nullptr;

	auto timestamp_millis = Timestamp::GetEpochMs(timestamp);
	for (auto &it : snapshots) {
		auto &snapshot = it.second;
		auto curr_millis = Timestamp::GetEpochMs(snapshot.timestamp_ms);
		if (curr_millis <= timestamp_millis && curr_millis >= max_millis) {
			max_snapshot = snapshot;
			max_millis = curr_millis;
		}
	}
	return max_snapshot;
}

shared_ptr<IcebergTableSchema> IcebergTableMetadata::GetSchemaFromId(int32_t schema_id) const {
	auto it = schemas.find(schema_id);
	D_ASSERT(it != schemas.end());
	return it->second;
}

optional_ptr<const IcebergPartitionSpec> IcebergTableMetadata::FindPartitionSpecById(int32_t spec_id) const {
	auto it = partition_specs.find(spec_id);
	D_ASSERT(it != partition_specs.end());
	return it->second;
}

optional_ptr<IcebergSnapshot> IcebergTableMetadata::GetLatestSnapshot() {
	if (!has_current_snapshot) {
		return nullptr;
	}
	auto latest_snapshot = GetSnapshotById(current_snapshot_id);
	return latest_snapshot;
}

const IcebergTableSchema &IcebergTableMetadata::GetLatestSchema() const {
	auto res = GetSchemaFromId(current_schema_id);
	D_ASSERT(res);
	return *res;
}

const IcebergPartitionSpec &IcebergTableMetadata::GetLatestPartitionSpec() const {
	auto res = FindPartitionSpecById(default_spec_id);
	D_ASSERT(res);
	return *res;
}

optional_ptr<IcebergSnapshot> IcebergTableMetadata::GetSnapshotById(int64_t snapshot_id) {
	auto snapshot = FindSnapshotByIdInternal(snapshot_id);
	if (!snapshot) {
		throw InvalidConfigurationException("Could not find snapshot with id " + to_string(snapshot_id));
	}
	return snapshot;
}

optional_ptr<IcebergSnapshot> IcebergTableMetadata::GetSnapshotByTimestamp(timestamp_t timestamp) {
	auto snapshot = FindSnapshotByIdTimestampInternal(timestamp);
	if (!snapshot) {
		throw InvalidConfigurationException("Could not find latest snapshots for timestamp " +
		                                    Timestamp::ToString(timestamp));
	}
	return snapshot;
}

optional_ptr<IcebergSnapshot> IcebergTableMetadata::GetSnapshot(const IcebergSnapshotLookup &lookup) {
	switch (lookup.snapshot_source) {
	case SnapshotSource::LATEST:
		return GetLatestSnapshot();
	case SnapshotSource::FROM_ID:
		return GetSnapshotById(lookup.snapshot_id);
	case SnapshotSource::FROM_TIMESTAMP:
		return GetSnapshotByTimestamp(lookup.snapshot_timestamp);
	default:
		throw InternalException("SnapshotSource type not implemented");
	}
}

//! ----------- Find Metadata -----------

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

string IcebergTableMetadata::GetTableVersionFromHint(const string &meta_path, FileSystem &fs,
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

bool IcebergTableMetadata::UnsafeVersionGuessingEnabled(ClientContext &context) {
	Value result;
	(void)context.TryGetCurrentSetting(VERSION_GUESSING_CONFIG_VARIABLE, result);
	return !result.IsNull() && result.GetValue<bool>();
}

string IcebergTableMetadata::GuessTableVersion(const string &meta_path, FileSystem &fs, const IcebergOptions &options) {
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

string IcebergTableMetadata::PickTableVersion(vector<OpenFileInfo> &found_metadata, string &version_pattern,
                                              string &glob) {
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

string IcebergTableMetadata::GetMetaDataPath(ClientContext &context, const string &path, FileSystem &fs,
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

//! ----------- Parse the Metadata JSON -----------

rest_api_objects::TableMetadata IcebergTableMetadata::Parse(const string &path, FileSystem &fs,
                                                            const string &metadata_compression_codec) {
	string json_content;
	if (metadata_compression_codec == "gzip" || StringUtil::EndsWith(path, "gz.metadata.json")) {
		json_content = IcebergUtils::GzFileToString(path, fs);
	} else {
		json_content = IcebergUtils::FileToString(path, fs);
	}
	auto doc = std::unique_ptr<yyjson_doc, YyjsonDocDeleter>(yyjson_read(json_content.c_str(), json_content.size(), 0));
	if (!doc) {
		throw InvalidInputException("Fails to parse iceberg metadata from %s", path);
	}

	auto root = yyjson_doc_get_root(doc.get());
	return rest_api_objects::TableMetadata::FromJSON(root);
}

IcebergTableMetadata IcebergTableMetadata::FromTableMetadata(rest_api_objects::TableMetadata &table_metadata) {
	IcebergTableMetadata res;

	res.iceberg_version = table_metadata.format_version;
	for (auto &schema : table_metadata.schemas) {
		res.schemas.emplace(schema.object_1.schema_id, IcebergTableSchema::ParseSchema(schema));
	}
	for (auto &snapshot : table_metadata.snapshots) {
		res.snapshots.emplace(snapshot.snapshot_id, IcebergSnapshot::ParseSnapshot(snapshot, res));
	}
	for (auto &spec : table_metadata.partition_specs) {
		res.partition_specs.emplace(spec.spec_id, IcebergPartitionSpec::ParseFromJson(spec));
	}
	if (!table_metadata.has_current_schema_id) {
		if (res.iceberg_version == 1) {
			throw NotImplementedException("Reading of the V1 'schema' field is not currently supported");
		}
		throw InvalidConfigurationException("'current_schema_id' field is missing from the metadata.json file");
	}
	res.current_schema_id = table_metadata.current_schema_id;
	if (table_metadata.has_current_snapshot_id && table_metadata.current_snapshot_id != -1) {
		res.has_current_snapshot = true;
		res.current_snapshot_id = table_metadata.current_snapshot_id;
	} else {
		res.has_current_snapshot = false;
	}
	res.last_sequence_number = table_metadata.last_sequence_number;
	res.default_spec_id = table_metadata.default_spec_id;

	auto &properties = table_metadata.properties;
	auto name_mapping = properties.find("schema.name-mapping.default");
	if (name_mapping != properties.end()) {
		auto doc = std::unique_ptr<yyjson_doc, YyjsonDocDeleter>(
		    yyjson_read(name_mapping->second.c_str(), name_mapping->second.size(), 0));
		if (doc == nullptr) {
			throw InvalidInputException("Fails to parse iceberg metadata 'schema.name-mapping.default' property");
		}
		auto root = yyjson_doc_get_root(doc.get());
		idx_t mapping_index = 0;
		res.mappings.emplace_back();
		mapping_index++;
		IcebergFieldMapping::ParseFieldMappings(root, res.mappings, mapping_index, 0);
	}
	return res;
}

} // namespace duckdb
