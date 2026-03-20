#include "core/metadata/iceberg_table_metadata.hpp"

#include "duckdb/common/exception.hpp"

#include "common/iceberg_utils.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "rest_catalog/objects/list.hpp"
#include "catalog/rest/api/iceberg_create_table_request.hpp"

namespace duckdb {

//! ----------- Select Snapshot -----------

optional_ptr<const IcebergSnapshot> IcebergTableMetadata::FindSnapshotByIdInternal(int64_t target_id) const {
	auto it = snapshots.find(target_id);
	if (it == snapshots.end()) {
		return nullptr;
	}
	return it->second;
}

optional_ptr<const IcebergSnapshot>
IcebergTableMetadata::FindSnapshotByIdTimestampInternal(timestamp_t timestamp) const {
	uint64_t max_millis = NumericLimits<uint64_t>::Minimum();
	optional_ptr<const IcebergSnapshot> max_snapshot = nullptr;

	auto timestamp_millis = Timestamp::GetEpochMs(timestamp);
	for (auto &it : snapshots) {
		auto &snapshot = it.second;
		auto curr_millis = Timestamp::GetEpochMs(snapshot.timestamp_ms);
		if (curr_millis <= timestamp_millis && static_cast<uint64_t>(curr_millis) >= max_millis) {
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

optional_ptr<const IcebergSortOrder> IcebergTableMetadata::FindSortOrderById(int32_t sort_id) const {
	auto it = sort_specs.find(sort_id);
	D_ASSERT(it != sort_specs.end());
	return it->second;
}

const unordered_map<int32_t, IcebergSortOrder> &IcebergTableMetadata::GetSortOrderSpecs() const {
	return sort_specs;
}

const unordered_map<int32_t, IcebergPartitionSpec> &IcebergTableMetadata::GetPartitionSpecs() const {
	return partition_specs;
}

optional_ptr<const IcebergSnapshot> IcebergTableMetadata::GetLatestSnapshot() const {
	if (!has_current_snapshot) {
		return nullptr;
	}
	return GetSnapshotById(current_snapshot_id);
}

const IcebergTableSchema &IcebergTableMetadata::GetLatestSchema() const {
	auto res = GetSchemaFromId(current_schema_id);
	D_ASSERT(res);
	return *res;
}

bool IcebergTableMetadata::HasPartitionSpec() const {
	auto spec = GetLatestPartitionSpec();
	return !spec.fields.empty();
}

const IcebergPartitionSpec &IcebergTableMetadata::GetLatestPartitionSpec() const {
	auto res = FindPartitionSpecById(default_spec_id);
	D_ASSERT(res);
	return *res;
}

bool IcebergTableMetadata::HasSortOrder() const {
	return default_sort_order_id.IsValid();
}

const IcebergSortOrder &IcebergTableMetadata::GetLatestSortOrder() const {
	D_ASSERT(HasSortOrder());
	auto sort_order_id = default_sort_order_id.GetIndex();
	auto res = FindSortOrderById(sort_order_id);
	D_ASSERT(res);
	return *res;
}

optional_ptr<const IcebergSnapshot> IcebergTableMetadata::GetSnapshotById(int64_t snapshot_id) const {
	auto snapshot = FindSnapshotByIdInternal(snapshot_id);
	if (!snapshot) {
		throw InvalidConfigurationException("Could not find snapshot with id " + to_string(snapshot_id));
	}
	return snapshot;
}

optional_ptr<const IcebergSnapshot> IcebergTableMetadata::GetSnapshotByTimestamp(timestamp_t timestamp) const {
	auto snapshot = FindSnapshotByIdTimestampInternal(timestamp);
	if (!snapshot) {
		throw InvalidConfigurationException("Could not find latest snapshots for timestamp " +
		                                    Timestamp::ToString(timestamp));
	}
	return snapshot;
}

optional_ptr<const IcebergSnapshot> IcebergTableMetadata::GetSnapshot(const IcebergSnapshotLookup &lookup) const {
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
// default format is "v%s%s.metadata.json" -> v00###-xxxxxxxxx-.gz.metadata.json
static string GenerateMetaDataUrl(FileSystem &fs, const string &meta_path, string &table_version,
                                  const IcebergOptions &options) {
	// TODO: Need to URL Encode table_version
	string compression_suffix = "";
	string url;
	if (options.metadata_compression_codec == "gzip") {
		compression_suffix = ".gz";
	}
	auto version_name_formats = StringUtil::Split(options.version_name_format, ',');
	vector<string> tried_paths;
	for (auto try_format : version_name_formats) {
		url = fs.JoinPath(meta_path, StringUtil::Format(try_format, table_version, compression_suffix));
		tried_paths.push_back(url);
		if (fs.FileExists(url)) {
			return url;
		}
	}

	string error;
	error = StringUtil::Format("Iceberg metadata file not found for table version '%s' using '%s' compression and "
	                           "format(s): '%s', tried paths:\n",
	                           table_version, options.metadata_compression_codec, options.version_name_format);
	error += StringUtil::Join(tried_paths, "\n");
	throw InvalidConfigurationException(error);
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

bool IcebergTableMetadata::HasLastAssignedColumnFieldId() const {
	return last_column_id.IsValid();
}

idx_t IcebergTableMetadata::GetLastAssignedColumnFieldId() const {
	return last_column_id.GetIndex();
}

bool IcebergTableMetadata::HasLastPartitionId() const {
	return last_partition_field_id.IsValid();
}

int32_t IcebergTableMetadata::GetLastPartitionFieldId() const {
	D_ASSERT(HasLastPartitionId());
	return static_cast<int32_t>(last_partition_field_id.GetIndex());
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

IcebergTableMetadata
IcebergTableMetadata::FromLoadTableResult(const rest_api_objects::LoadTableResult &load_table_result) {
	auto res = FromTableMetadata(load_table_result.metadata);
	res.latest_metadata_json = load_table_result.metadata_location;
	return res;
}

IcebergTableMetadata IcebergTableMetadata::FromTableMetadata(const rest_api_objects::TableMetadata &table_metadata) {
	IcebergTableMetadata res;

	res.table_uuid = table_metadata.table_uuid;
	res.location = table_metadata.location;
	res.iceberg_version = table_metadata.format_version;
	res.last_updated_ms = timestamp_t(table_metadata.last_updated_ms);
	for (auto &schema : table_metadata.schemas) {
		res.schemas.emplace(schema.object_1.schema_id, IcebergTableSchema::ParseSchema(schema));
	}
	for (auto &snapshot : table_metadata.snapshots) {
		res.snapshots.emplace(snapshot.snapshot_id, IcebergSnapshot::ParseSnapshot(snapshot, res));
	}
	for (auto &spec : table_metadata.partition_specs) {
		res.partition_specs.emplace(spec.spec_id, IcebergPartitionSpec::ParseFromJson(spec));
	}
	for (auto &sort_order : table_metadata.sort_orders) {
		res.sort_specs.emplace(sort_order.order_id, IcebergSortOrder::ParseFromJson(sort_order));
	}
	if (!table_metadata.has_current_schema_id) {
		if (res.iceberg_version == 1) {
			throw NotImplementedException("Reading of the V1 'schema' field is not currently supported");
		}
		throw InvalidConfigurationException("'current_schema_id' field is missing from the metadata.json file");
	}
	res.current_schema_id = table_metadata.current_schema_id;
	if (table_metadata.has_next_row_id) {
		res.has_next_row_id = true;
		res.next_row_id = table_metadata.next_row_id;
	}

	if (table_metadata.has_current_snapshot_id && table_metadata.current_snapshot_id != -1) {
		res.has_current_snapshot = true;
		res.current_snapshot_id = table_metadata.current_snapshot_id;
	} else {
		res.has_current_snapshot = false;
	}

	if (table_metadata.has_last_sequence_number) {
		res.last_sequence_number = table_metadata.last_sequence_number;
	} else {
		//! SPEC: Table metadata field last-sequence-number must default to 0
		res.last_sequence_number = 0;
	}

	res.default_spec_id = table_metadata.default_spec_id;
	if (table_metadata.has_default_sort_order_id) {
		res.default_sort_order_id = table_metadata.default_sort_order_id;
	}

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

	// parse all table properties
	for (auto &property : properties) {
		res.table_properties.emplace(property.first, property.second);
	}

	if (table_metadata.has_last_column_id) {
		res.last_column_id = table_metadata.last_column_id;
	}

	if (table_metadata.has_last_partition_id) {
		res.last_partition_field_id = table_metadata.last_partition_id;
	}

	return res;
}

const case_insensitive_map_t<string> &IcebergTableMetadata::GetTableProperties() const {
	return table_properties;
}

const string &IcebergTableMetadata::GetLatestMetadataJson() const {
	return latest_metadata_json;
}

const string &IcebergTableMetadata::GetLocation() const {
	return location;
}

const string IcebergTableMetadata::GetDataPath(FileSystem &fs) const {
	auto write_path = table_properties.find("write.data.path");
	// If write.data.path property is set, use it; otherwise use default location + "/data"
	if (write_path != table_properties.end()) {
		return write_path->second;
	}
	return fs.JoinPath(location, "data");
}

const string IcebergTableMetadata::GetMetadataPath(FileSystem &fs) const {
	// If write.metadata.path property is set, use it; otherwise use default location + "/metadata"
	auto metadata_path = table_properties.find("write.metadata.path");
	// If write.data.path property is set, use it; otherwise use default location + "/metadata"
	if (metadata_path != table_properties.end()) {
		return metadata_path->second;
	}
	return fs.JoinPath(location, "metadata");
}

string IcebergTableMetadata::GetTableProperty(string property_string) const {
	auto prop = table_properties.find(property_string);
	if (prop != table_properties.end()) {
		return prop->second;
	}
	return "";
}

bool IcebergTableMetadata::PropertiesAllowPositionalDeletes(IcebergSnapshotOperationType operation_type) const {
	// first check write.delete.mode. If not present go to write.update.mode
	switch (operation_type) {
	case IcebergSnapshotOperationType::DELETE: {
		auto delete_mode = GetTableProperty("write.delete.mode");
		// if unset or merge-on-read, it supports positional deletes
		return delete_mode == "merge-on-read" || delete_mode.empty();
	}
	case IcebergSnapshotOperationType::OVERWRITE: {
		// if unset or merge-on-read, it supports positional deletes
		auto update_mode = GetTableProperty("write.update.mode");
		return update_mode == "merge-on-read" || update_mode.empty();
	}
	default:
		throw NotImplementedException("Operation type not supported");
	}
}

yyjson_mut_val *IcebergTableMetadata::SchemasToJSON(yyjson_mut_doc *doc) const {
	auto schemas_array = yyjson_mut_arr(doc);
	for (auto &it : schemas) {
		auto &schema = *it.second;
		auto schema_obj = yyjson_mut_obj(doc);
		IcebergCreateTableRequest::PopulateSchema(doc, schema_obj, schema);
		yyjson_mut_arr_add_val(schemas_array, schema_obj);
	}
	return schemas_array;
}

yyjson_mut_val *IcebergTableMetadata::PartitionsToJSON(yyjson_mut_doc *doc) const {
	auto partitions_array = yyjson_mut_arr(doc);
	for (auto &it : partition_specs) {
		auto &partition_spec = it.second;
		auto partition_obj = partition_spec.ToJSON(doc);
		yyjson_mut_arr_add_val(partitions_array, partition_obj);
	}
	return partitions_array;
}

yyjson_mut_val *IcebergTableMetadata::TablePropertiesToJSON(yyjson_mut_doc *doc) const {
	auto properties_obj = yyjson_mut_obj(doc);
	for (auto &property : table_properties) {
		auto &key = property.first;
		auto &val = property.second;
		//! Register the string as part of the document, to ensure lifetime correctness
		auto key_val = unsafe_yyjson_mut_strncpy(doc, key.c_str(), key.size());
		yyjson_mut_obj_add_strncpy(doc, properties_obj, key_val, val.c_str(), val.size());
	}
	return properties_obj;
}

yyjson_mut_val *IcebergTableMetadata::SnapshotsToJSON(yyjson_mut_doc *doc) const {
	auto snapshots_array = yyjson_mut_arr(doc);
	for (auto &it : snapshots) {
		auto &snapshot = it.second;
		auto snapshot_rest_object = snapshot.ToRESTObject(*this);
		auto snapshot_obj = IcebergSnapshot::ToJSON(snapshot_rest_object, doc);
		yyjson_mut_arr_add_val(snapshots_array, snapshot_obj);
	}
	return snapshots_array;
}

yyjson_mut_val *IcebergTableMetadata::SnapshotLogToJSON(yyjson_mut_doc *doc) const {
	auto log_array = yyjson_mut_arr(doc);
	for (auto &it : snapshots) {
		auto &snapshot = it.second;
		auto log_item = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_int(doc, log_item, "snapshot-id", snapshot.snapshot_id);
		yyjson_mut_obj_add_int(doc, log_item, "timestamp-ms", snapshot.timestamp_ms.value);
		yyjson_mut_arr_add_val(log_array, log_item);
	}
	return log_array;
}

yyjson_mut_val *IcebergTableMetadata::SortOrdersToJSON(yyjson_mut_doc *doc) const {
	auto sort_orders_array = yyjson_mut_arr(doc);
	for (auto &it : sort_specs) {
		auto &sort_order = it.second;
		auto sort_order_obj = sort_order.ToJSON(doc);
		yyjson_mut_arr_add_val(sort_orders_array, sort_order_obj);
	}
	return sort_orders_array;
}

string IcebergTableMetadata::ToJSON() const {
	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
	auto doc = doc_p.get();
	auto root_obj = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root_obj);

	yyjson_mut_obj_add_val(doc, root_obj, "format-version", yyjson_mut_int(doc, iceberg_version));
	yyjson_mut_obj_add_val(doc, root_obj, "table-uuid", yyjson_mut_str(doc, table_uuid.c_str()));
	yyjson_mut_obj_add_val(doc, root_obj, "location", yyjson_mut_str(doc, location.c_str()));
	yyjson_mut_obj_add_val(doc, root_obj, "last-updated-ms", yyjson_mut_int(doc, last_updated_ms.value));
	yyjson_mut_obj_add_val(doc, root_obj, "last-column-id", yyjson_mut_int(doc, last_column_id.GetIndex()));
	yyjson_mut_obj_add_val(doc, root_obj, "schemas", SchemasToJSON(doc));
	yyjson_mut_obj_add_val(doc, root_obj, "current-schema-id", yyjson_mut_int(doc, current_schema_id));
	yyjson_mut_obj_add_val(doc, root_obj, "partition-specs", PartitionsToJSON(doc));
	yyjson_mut_obj_add_val(doc, root_obj, "default-spec-id", yyjson_mut_int(doc, default_spec_id));
	yyjson_mut_obj_add_val(doc, root_obj, "last-partition-id", yyjson_mut_int(doc, last_partition_field_id.GetIndex()));
	yyjson_mut_obj_add_val(doc, root_obj, "properties", TablePropertiesToJSON(doc));
	yyjson_mut_obj_add_val(doc, root_obj, "current-snapshot-id", yyjson_mut_int(doc, current_snapshot_id));
	yyjson_mut_obj_add_val(doc, root_obj, "snapshots", SnapshotsToJSON(doc));
	yyjson_mut_obj_add_val(doc, root_obj, "snapshot-log", SnapshotLogToJSON(doc));
	// yyjson_mut_obj_add_val(doc, root_obj, "metadata-log", MetadataLogToJSON(doc));
	yyjson_mut_obj_add_val(doc, root_obj, "sort-orders", SortOrdersToJSON(doc));
	yyjson_mut_obj_add_val(doc, root_obj, "default-sort-order-id",
	                       yyjson_mut_int(doc, default_sort_order_id.GetIndex()));
	// yyjson_mut_obj_add_val(doc, root_obj, "refs", RefToJSON(doc));
	// yyjson_mut_obj_add_val(doc, root_obj, "encryption-keys", EncryptionKeyToJSON(doc));
	return ICUtils::JsonToString(std::move(doc_p));
}

void IcebergTableMetadata::WriteMetadata(ClientContext &context, const string &path) const {
	auto &fs = FileSystem::GetFileSystem(context);

	// Generate JSON using ToJSON()
	auto json_content = ToJSON();

	// Write to file
	auto file = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	file->Write((void *)json_content.c_str(), json_content.size());
	file->Close();
}

void IcebergTableMetadata::WriteVersionHint(ClientContext &context, const string &path,
                                            const string &version_hint) const {
	auto &fs = FileSystem::GetFileSystem(context);

	// Write to file
	auto file = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	file->Write((void *)version_hint.c_str(), version_hint.size());
	file->Close();
}

} // namespace duckdb
