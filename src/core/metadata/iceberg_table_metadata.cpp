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

optional_ptr<const IcebergSnapshot> IcebergTableMetadata::GetSnapshotByTimestampMS(timestamp_ms_t timestamp) const {
	// Per Iceberg spec, point-in-time resolution should use snapshot-log (the
	// history of refs.main). Searching the global snapshots map would incorrectly
	// pick side-branch tips - see duckdb-iceberg#969.
	//
	// All comparisons below are in raw epoch-millis: snapshot_log stores ms, and
	// we convert the incoming lookup timestamp to ms once here.
	if (!snapshot_log.empty()) {
		// snapshot_log is sorted ascending by timestamp_ms; walk newest-first and
		// return the first entry whose snapshot still exists in the snapshots map
		// (spec allows expired snapshots to leave dangling log entries).
		for (auto it = snapshot_log.rbegin(); it != snapshot_log.rend(); ++it) {
			if (it->second <= timestamp) {
				if (auto snap = FindSnapshotByIdInternal(it->first)) {
					return snap;
				}
			}
		}
		return nullptr;
	}

	// scan all snapshots for the largest timestamp_ms <= target.
	optional_ptr<const IcebergSnapshot> max_snapshot = nullptr;
	for (auto &it : snapshots) {
		auto &snapshot = it.second;
		const bool can_be_seen_by_transaction = snapshot.timestamp_ms <= timestamp;
		if (!can_be_seen_by_transaction) {
			continue;
		}
		if (!max_snapshot || snapshot.timestamp_ms >= max_snapshot->timestamp_ms) {
			max_snapshot = snapshot;
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
	if (!current_snapshot_id) {
		return nullptr;
	}
	return GetSnapshotById(*current_snapshot_id);
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

IcebergSnapshotScanInfo IcebergTableMetadata::GetSnapshot(const IcebergSnapshotLookup &lookup) const {
	IcebergSnapshotScanInfo snapshot_info;
	switch (lookup.GetSource()) {
	case SnapshotSource::LATEST:
		snapshot_info.snapshot = GetLatestSnapshot();
		snapshot_info.schema_id = GetCurrentSchemaId();
		return snapshot_info;
	case SnapshotSource::FROM_ID:
		snapshot_info.snapshot = GetSnapshotById(lookup.GetSnapshotId());
		snapshot_info.schema_id = snapshot_info.snapshot->GetSchemaId();
		return snapshot_info;
	case SnapshotSource::FROM_TIMESTAMP:
		snapshot_info.snapshot = GetSnapshotByTimestampMS(lookup.GetSnapshotTimestamp());
		if (snapshot_info.snapshot) {
			snapshot_info.schema_id = snapshot_info.snapshot->GetSchemaId();
		} else {
			snapshot_info.schema_id = GetCurrentSchemaId();
		}
		return snapshot_info;
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

bool IcebergTableMetadata::HasLastColumnId() const {
	return last_column_id.IsValid();
}

idx_t IcebergTableMetadata::GetLastColumnId() const {
	return last_column_id.GetIndex();
}

void IcebergTableMetadata::SetCurrentSchemaId(int32_t value) {
	current_schema_id = value;
}

int32_t IcebergTableMetadata::GetCurrentSchemaId() const {
	return current_schema_id;
}

IcebergTableSchema &IcebergTableMetadata::AddSchemaOrGetExisting(shared_ptr<IcebergTableSchema> schema) {
	for (auto &it : schemas) {
		auto &item = *it.second;
		if (schema->Equals(item)) {
			return item;
		}
	}
	auto new_schema_id = schema->schema_id;
	auto res = schemas.emplace(new_schema_id, std::move(schema));
	if (!res.second) {
		throw InvalidConfigurationException("Attempted to add schema with id %d, but this already exists in the table!",
		                                    new_schema_id);
	}
	return *res.first->second;
}

const unordered_map<int32_t, shared_ptr<IcebergTableSchema>> &IcebergTableMetadata::GetSchemas() const {
	return schemas;
}

optional_ptr<const IcebergColumnDefinition> IcebergTableMetadata::FindColumnByFieldId(int32_t field_id) const {
	for (auto &schema_entry : schemas) {
		auto &schema = *schema_entry.second;
		auto column = schema.TryGetColumnByFieldId(field_id);
		if (column) {
			return column;
		}
	}
	return nullptr;
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
	auto doc = JSONDocument::Parse(json_content.c_str(), json_content.size());
	return rest_api_objects::TableMetadata::FromJSON(doc->GetRoot());
}

IcebergTableMetadata IcebergTableMetadata::FromTableMetadata(const rest_api_objects::TableMetadata &table_metadata) {
	IcebergTableMetadata res;

	res.table_uuid = table_metadata.table_uuid;
	D_ASSERT(table_metadata.location);
	res.location = *table_metadata.location;
	res.iceberg_version = table_metadata.format_version;
	D_ASSERT(table_metadata.last_updated_ms);
	res.last_updated_ms = timestamp_ms_t(*table_metadata.last_updated_ms);
	if (table_metadata.schemas) {
		for (auto &schema : *table_metadata.schemas) {
			D_ASSERT(schema.object_1.schema_id);
			res.schemas.emplace(*schema.object_1.schema_id, IcebergTableSchema::ParseSchema(schema));
		}
	} else if (res.iceberg_version == 1 && table_metadata.schema) {
		auto schema = table_metadata.schema->Copy();
		if (!schema.object_1.schema_id) {
			schema.object_1.schema_id = 0;
		}
		res.schemas.emplace(*schema.object_1.schema_id, IcebergTableSchema::ParseSchema(schema));
	}
	if (table_metadata.snapshots) {
		for (auto &snapshot : *table_metadata.snapshots) {
			res.snapshots.emplace(snapshot.snapshot_id, IcebergSnapshot::ParseSnapshot(snapshot, res));
		}
	}
	if (table_metadata.snapshot_log) {
		res.snapshot_log.reserve(table_metadata.snapshot_log->value.size());
		for (auto &entry : table_metadata.snapshot_log->value) {
			res.snapshot_log.emplace_back(entry.snapshot_id, entry.timestamp_ms);
		}
		std::sort(res.snapshot_log.begin(), res.snapshot_log.end(),
		          [](const pair<int64_t, timestamp_ms_t> &a, const pair<int64_t, timestamp_ms_t> &b) {
			          return a.second < b.second;
		          });
	}
	if (table_metadata.partition_specs) {
		for (auto &spec : *table_metadata.partition_specs) {
			D_ASSERT(spec.spec_id);
			res.partition_specs.emplace(*spec.spec_id, IcebergPartitionSpec::ParseFromJson(spec));
		}
	} else if (res.iceberg_version == 1 && table_metadata.partition_spec) {
		rest_api_objects::PartitionSpec spec;
		spec.spec_id = 0;
		for (auto &field : *table_metadata.partition_spec) {
			spec.fields.emplace_back(field.Copy());
		}
		res.partition_specs.emplace(0, IcebergPartitionSpec::ParseFromJson(spec));
	}
	if (table_metadata.sort_orders) {
		for (auto &sort_order : *table_metadata.sort_orders) {
			res.sort_specs.emplace(sort_order.order_id, IcebergSortOrder::ParseFromJson(sort_order));
		}
	}
	if (!table_metadata.current_schema_id) {
		if (res.iceberg_version == 1 && table_metadata.schema) {
			res.current_schema_id = table_metadata.schema->object_1.schema_id.value_or(0);
		} else {
			throw InvalidConfigurationException("'current_schema_id' field is missing from the metadata.json file");
		}
	} else {
		res.current_schema_id = *table_metadata.current_schema_id;
	}
	if (table_metadata.next_row_id) {
		res.next_row_id = *table_metadata.next_row_id;
	}

	if (table_metadata.current_snapshot_id && *table_metadata.current_snapshot_id != -1) {
		res.current_snapshot_id = *table_metadata.current_snapshot_id;
	}

	if (table_metadata.last_sequence_number) {
		res.last_sequence_number = *table_metadata.last_sequence_number;
	} else {
		//! SPEC: Table metadata field last-sequence-number must default to 0
		res.last_sequence_number = 0;
	}

	if (table_metadata.default_spec_id) {
		res.default_spec_id = *table_metadata.default_spec_id;
	} else if (res.iceberg_version == 1 && table_metadata.partition_spec) {
		res.default_spec_id = 0;
	} else {
		throw InvalidConfigurationException("'default-spec-id' field is missing from the metadata.json file");
	}
	if (table_metadata.default_sort_order_id) {
		res.default_sort_order_id = *table_metadata.default_sort_order_id;
	}

	if (table_metadata.properties) {
		auto &properties = *table_metadata.properties;
		auto name_mapping = properties.find("schema.name-mapping.default");
		if (name_mapping != properties.end()) {
			auto doc = JSONDocument::Parse(name_mapping->second.c_str(), name_mapping->second.size());
			auto root = doc->GetRoot();
			idx_t mapping_index = 0;
			res.mappings.emplace_back();
			mapping_index++;
			IcebergFieldMapping::ParseFieldMappings(root, res.mappings, mapping_index, 0);
		}

		// parse all table properties
		for (auto &property : properties) {
			res.table_properties.emplace(property.first, property.second);
		}
	}

	if (table_metadata.last_column_id) {
		res.last_column_id = *table_metadata.last_column_id;
	}

	if (table_metadata.last_partition_id) {
		res.last_partition_field_id = *table_metadata.last_partition_id;
	}

	if (table_metadata.metadata_log) {
		for (auto &item : table_metadata.metadata_log->value) {
			res.metadata_log.emplace_back(item.metadata_file, timestamp_ms_t(item.timestamp_ms));
		}
	}
	return res;
}

IcebergTableMetadata IcebergTableMetadata::Copy() const {
	IcebergTableMetadata res;
	res.table_uuid = table_uuid;
	res.location = location;
	res.iceberg_version = iceberg_version;
	res.default_spec_id = default_spec_id;
	res.next_row_id = next_row_id;
	res.default_sort_order_id = default_sort_order_id;
	res.current_snapshot_id = current_snapshot_id;
	res.last_sequence_number = last_sequence_number;
	res.last_updated_ms = last_updated_ms;
	res.last_column_id = last_column_id;
	res.last_partition_field_id = last_partition_field_id;
	res.partition_specs = partition_specs;
	res.sort_specs = sort_specs;
	res.snapshots = snapshots;
	res.snapshot_log = snapshot_log;
	res.mappings = mappings;
	res.write_data_path = write_data_path;
	res.write_metadata_path = write_metadata_path;
	res.table_properties = table_properties;
	res.metadata_log = metadata_log;
	res.current_schema_id = current_schema_id;
	res.schemas = schemas;
	return res;
}

const case_insensitive_map_t<string> &IcebergTableMetadata::GetTableProperties() const {
	return table_properties;
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

JSONMutableValue IcebergTableMetadata::SchemasToJSON(JSONWriter &writer) const {
	auto schemas_array = writer.CreateArray();
	for (auto &it : schemas) {
		auto &schema = *it.second;
		auto schema_obj = writer.CreateObject();
		IcebergCreateTableRequest::PopulateSchema(writer, schema_obj, schema);
		schemas_array.Append(schema_obj);
	}
	return schemas_array;
}

JSONMutableValue IcebergTableMetadata::PartitionsToJSON(JSONWriter &writer) const {
	auto partitions_array = writer.CreateArray();
	for (auto &it : partition_specs) {
		auto &partition_spec = it.second;
		partitions_array.Append(partition_spec.ToJSON(writer));
	}
	return partitions_array;
}

JSONMutableValue IcebergTableMetadata::TablePropertiesToJSON(JSONWriter &writer) const {
	auto properties_obj = writer.CreateObject();
	for (auto &property : table_properties) {
		auto &key = property.first;
		auto &val = property.second;
		properties_obj.AddString(key, val);
	}
	return properties_obj;
}

JSONMutableValue IcebergTableMetadata::SnapshotsToJSON(JSONWriter &writer) const {
	auto snapshots_array = writer.CreateArray();
	for (auto &it : snapshots) {
		auto &snapshot = it.second;
		auto snapshot_rest_object = snapshot.ToRESTObject(*this);
		snapshots_array.Append(snapshot_rest_object.ToJSON(writer));
	}
	return snapshots_array;
}

JSONMutableValue IcebergTableMetadata::SnapshotLogToJSON(JSONWriter &writer) const {
	auto log_array = writer.CreateArray();
	for (auto &it : snapshots) {
		auto &snapshot = it.second;
		auto log_item = writer.CreateObject();
		if (!snapshot.snapshot_id) {
			throw InvalidConfigurationException("snapshot.snapshot_id is not set");
		}
		log_item.Add("snapshot-id", writer.CreateSignedInteger(*snapshot.snapshot_id));
		log_item.Add("timestamp-ms", writer.CreateSignedInteger(snapshot.timestamp_ms.value));
		log_array.Append(log_item);
	}
	return log_array;
}

JSONMutableValue IcebergTableMetadata::SortOrdersToJSON(JSONWriter &writer) const {
	auto sort_orders_array = writer.CreateArray();
	for (auto &it : sort_specs) {
		auto &sort_order = it.second;
		sort_orders_array.Append(sort_order.ToJSON(writer));
	}
	return sort_orders_array;
}

string IcebergTableMetadata::ToJSON() const {
	JSONWriter writer;
	auto root_obj = writer.CreateObject();
	writer.SetRoot(root_obj);

	root_obj.Add("format-version", writer.CreateSignedInteger(iceberg_version));
	root_obj.AddString("table-uuid", table_uuid);
	root_obj.AddString("location", location);
	root_obj.Add("last-updated-ms", writer.CreateSignedInteger(last_updated_ms.value));
	root_obj.Add("last-column-id", writer.CreateSignedInteger(last_column_id.GetIndex()));
	root_obj.Add("schemas", SchemasToJSON(writer));
	root_obj.Add("current-schema-id", writer.CreateSignedInteger(current_schema_id));
	root_obj.Add("partition-specs", PartitionsToJSON(writer));
	root_obj.Add("default-spec-id", writer.CreateSignedInteger(default_spec_id));
	root_obj.Add("last-partition-id", writer.CreateSignedInteger(last_partition_field_id.GetIndex()));
	root_obj.Add("properties", TablePropertiesToJSON(writer));
	if (current_snapshot_id) {
		root_obj.Add("current-snapshot-id", writer.CreateSignedInteger(*current_snapshot_id));
	}
	root_obj.Add("snapshots", SnapshotsToJSON(writer));
	root_obj.Add("snapshot-log", SnapshotLogToJSON(writer));
	root_obj.Add("sort-orders", SortOrdersToJSON(writer));
	root_obj.Add("default-sort-order-id", writer.CreateSignedInteger(default_sort_order_id.GetIndex()));
	return writer.ToString(JSONWriteFlags::ALLOW_INF_AND_NAN);
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
