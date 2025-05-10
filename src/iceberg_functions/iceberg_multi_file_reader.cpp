#include "iceberg_multi_file_reader.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_predicate.hpp"
#include "iceberg_predicate_stats.hpp"
#include "iceberg_value.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

namespace duckdb {

IcebergMultiFileList::IcebergMultiFileList(ClientContext &context_p, const string &path, const IcebergOptions &options)
    : MultiFileList({path}, FileGlobOptions::ALLOW_EMPTY), lock(), context(context_p), options(options) {
}

string IcebergMultiFileList::ToDuckDBPath(const string &raw_path) {
	return raw_path;
}

string IcebergMultiFileList::GetPath() const {
	return GetPaths()[0].path;
}

void IcebergMultiFileList::Bind(vector<LogicalType> &return_types, vector<string> &names) {
	lock_guard<mutex> guard(lock);

	if (have_bound) {
		names = this->names;
		return_types = this->types;
		return;
	}

	if (!initialized) {
		InitializeFiles(guard);
	}

	auto &schema = snapshot.schema;
	for (auto &schema_entry : schema) {
		names.push_back(schema_entry.name);
		return_types.push_back(schema_entry.type);
	}

	have_bound = true;
	this->names = names;
	this->types = return_types;
}

unique_ptr<IcebergMultiFileList> IcebergMultiFileList::PushdownInternal(ClientContext &context,
                                                                        TableFilterSet &new_filters) const {
	auto filtered_list = make_uniq<IcebergMultiFileList>(context, paths[0].path, this->options);

	TableFilterSet result_filter_set;

	// Add pre-existing filters
	for (auto &entry : table_filters.filters) {
		result_filter_set.PushFilter(ColumnIndex(entry.first), entry.second->Copy());
	}

	// Add new filters
	for (auto &entry : new_filters.filters) {
		if (entry.first < names.size()) {
			result_filter_set.PushFilter(ColumnIndex(entry.first), entry.second->Copy());
		}
	}

	filtered_list->table_filters = std::move(result_filter_set);
	filtered_list->names = names;
	filtered_list->types = types;
	filtered_list->have_bound = true;

	// Copy over the snapshot, this avoids reparsing metadata
	{
		unique_lock<mutex> lck(lock);
		filtered_list->snapshot = snapshot;
	}
	return filtered_list;
}

unique_ptr<MultiFileList>
IcebergMultiFileList::DynamicFilterPushdown(ClientContext &context, const MultiFileOptions &options,
                                            const vector<string> &names, const vector<LogicalType> &types,
                                            const vector<column_t> &column_ids, TableFilterSet &filters) const {
	if (filters.filters.empty()) {
		return nullptr;
	}

	TableFilterSet filters_copy;
	for (auto &filter : filters.filters) {
		auto column_id = column_ids[filter.first];
		auto previously_pushed_down_filter = this->table_filters.filters.find(column_id);
		if (previously_pushed_down_filter != this->table_filters.filters.end() &&
		    filter.second->Equals(*previously_pushed_down_filter->second)) {
			// Skip filters that we already have pushed down
			continue;
		}
		filters_copy.PushFilter(ColumnIndex(column_id), filter.second->Copy());
	}

	if (!filters_copy.filters.empty()) {
		auto new_snap = PushdownInternal(context, filters_copy);
		return std::move(new_snap);
	}
	return nullptr;
}

unique_ptr<MultiFileList> IcebergMultiFileList::ComplexFilterPushdown(ClientContext &context,
                                                                      const MultiFileOptions &options,
                                                                      MultiFilePushdownInfo &info,
                                                                      vector<unique_ptr<Expression>> &filters) {
	if (filters.empty()) {
		return nullptr;
	}

	FilterCombiner combiner(context);
	for (const auto &filter : filters) {
		combiner.AddFilter(filter->Copy());
	}

	vector<FilterPushdownResult> unused;
	auto filter_set = combiner.GenerateTableScanFilters(info.column_indexes, unused);
	if (filter_set.filters.empty()) {
		return nullptr;
	}

	return PushdownInternal(context, filter_set);
}

vector<OpenFileInfo> IcebergMultiFileList::GetAllFiles() {
	vector<OpenFileInfo> file_list;
	for (idx_t i = 0; i < data_files.size(); i++) {
		file_list.push_back(GetFile(i));
	}
	return file_list;
}

FileExpandResult IcebergMultiFileList::GetExpandResult() {
	// GetFile(1) will ensure files with index 0 and index 1 are expanded if they are available
	GetFile(1);

	if (data_files.size() > 1) {
		return FileExpandResult::MULTIPLE_FILES;
	} else if (data_files.size() == 1) {
		return FileExpandResult::SINGLE_FILE;
	}

	return FileExpandResult::NO_FILES;
}

idx_t IcebergMultiFileList::GetTotalFileCount() {
	// FIXME: the 'added_files_count' + the 'existing_files_count'
	// in the Manifest List should give us this information without scanning the manifest list
	idx_t i = data_files.size();
	while (!GetFile(i).path.empty()) {
		i++;
	}
	return data_files.size();
}

unique_ptr<NodeStatistics> IcebergMultiFileList::GetCardinality(ClientContext &context) {
	idx_t cardinality = 0;

	if (snapshot.iceberg_format_version == 1) {
		//! We collect no cardinality information from manifests for V1 tables.
		return nullptr;
	}

	//! Make sure we have fetched all manifests
	(void)GetTotalFileCount();

	for (idx_t i = 0; i < data_manifests.size(); i++) {
		cardinality += data_manifests[i].added_rows_count;
		cardinality += data_manifests[i].existing_rows_count;
	}
	for (idx_t i = 0; i < delete_manifests.size(); i++) {
		cardinality -= delete_manifests[i].added_rows_count;
	}
	return make_uniq<NodeStatistics>(cardinality, cardinality);
}

static void DeserializeBounds(const string &lower_bound, const string &upper_bound, const string &name,
                              const LogicalType &type, IcebergPredicateStats &out) {
	string_t lower_bound_blob(lower_bound.data(), lower_bound.size());
	string_t upper_bound_blob(upper_bound.data(), upper_bound.size());

	auto deserialized_lower_bound = IcebergValue::DeserializeValue(lower_bound_blob, type);
	auto deserialized_upper_bound = IcebergValue::DeserializeValue(upper_bound_blob, type);

	if (deserialized_lower_bound.HasError()) {
		throw InvalidConfigurationException("Column %s lower bound deserialization failed: %s", name,
		                                    deserialized_lower_bound.GetError());
	}
	if (deserialized_upper_bound.HasError()) {
		throw InvalidConfigurationException("Column %s upper bound deserialization failed: %s", name,
		                                    deserialized_upper_bound.GetError());
	}
	out.lower_bound = deserialized_lower_bound.GetValue();
	out.upper_bound = deserialized_upper_bound.GetValue();
}

bool IcebergMultiFileList::FileMatchesFilter(IcebergManifestEntry &file) {
	D_ASSERT(!table_filters.filters.empty());

	auto &filters = table_filters.filters;
	auto &schema = snapshot.schema;

	for (idx_t column_id = 0; column_id < schema.size(); column_id++) {
		// FIXME: is there a potential mismatch between column_id / field_id lurking here?
		auto &column = schema[column_id];
		auto it = filters.find(column_id);

		if (it == filters.end()) {
			continue;
		}
		if (file.lower_bounds.empty() || file.upper_bounds.empty()) {
			//! There are no bounds statistics for the file, can't filter
			continue;
		}

		auto &source_id = column.id;
		auto lower_bound_it = file.lower_bounds.find(source_id);
		auto upper_bound_it = file.upper_bounds.find(source_id);
		if (lower_bound_it == file.lower_bounds.end() || upper_bound_it == file.upper_bounds.end()) {
			//! There are no bound statistics for this column
			continue;
		}

		IcebergPredicateStats stats;
		DeserializeBounds(lower_bound_it->second, upper_bound_it->second, column.name, column.type, stats);

		auto &filter = *it->second;
		if (!IcebergPredicate::MatchBounds(filter, stats, IcebergTransform::Identity())) {
			//! If any predicate fails, exclude the file
			return false;
		}
	}
	return true;
}

OpenFileInfo IcebergMultiFileList::GetFile(idx_t file_id) {
	lock_guard<mutex> guard(lock);
	if (!initialized) {
		InitializeFiles(guard);
	}

	auto iceberg_path = GetPath();
	auto &fs = FileSystem::GetFileSystem(context);
	auto &data_files = this->data_files;
	auto &entry_producer = this->entry_producer;

	// Read enough data files
	while (file_id >= data_files.size()) {
		if (data_manifest_entry_reader->Finished()) {
			if (current_data_manifest == data_manifests.end()) {
				break;
			}
			auto &manifest = *current_data_manifest;
			auto manifest_entry_full_path = options.allow_moved_paths
			                                    ? IcebergUtils::GetFullPath(iceberg_path, manifest.manifest_path, fs)
			                                    : manifest.manifest_path;
			auto scan = make_uniq<AvroScan>("IcebergManifest", context, manifest_entry_full_path);
			data_manifest_entry_reader->Initialize(std::move(scan));
			data_manifest_entry_reader->SetSequenceNumber(manifest.sequence_number);
			data_manifest_entry_reader->SetPartitionSpecID(manifest.partition_spec_id);
		}

		idx_t remaining = (file_id + 1) - data_files.size();
		if (!table_filters.filters.empty()) {
			// FIXME: push down the filter into the 'read_avro' scan, so the entries that don't match are just filtered
			// out
			vector<IcebergManifestEntry> intermediate_entries;
			data_manifest_entry_reader->ReadEntries(
			    remaining, [&intermediate_entries, &entry_producer](DataChunk &chunk, idx_t offset, idx_t count,
			                                                        const ManifestReaderInput &input) {
				    return entry_producer(chunk, offset, count, input, intermediate_entries);
			    });

			for (auto &entry : intermediate_entries) {
				if (!FileMatchesFilter(entry)) {
					DUCKDB_LOG_INFO(context, "duckdb.Extensions.Iceberg",
					                "Iceberg Filter Pushdown, skipped 'data_file': '%s'", entry.file_path);
					//! Skip this file
					continue;
				}
				data_files.push_back(entry);
			}
		} else {
			data_manifest_entry_reader->ReadEntries(
			    remaining, [&data_files, &entry_producer](DataChunk &chunk, idx_t offset, idx_t count,
			                                              const ManifestReaderInput &input) {
				    return entry_producer(chunk, offset, count, input, data_files);
			    });
		}

		if (data_manifest_entry_reader->Finished()) {
			current_data_manifest++;
			continue;
		}
	}
#ifdef DEBUG
	for (auto &entry : data_files) {
		D_ASSERT(entry.content == IcebergManifestEntryContentType::DATA);
		D_ASSERT(entry.status != IcebergManifestEntryStatusType::DELETED);
	}
#endif

	if (file_id >= data_files.size()) {
		return OpenFileInfo();
	}

	D_ASSERT(file_id < data_files.size());
	const auto &data_file = data_files[file_id];
	const auto &path = data_file.file_path;

	if (!StringUtil::CIEquals(data_file.file_format, "parquet")) {
		throw NotImplementedException("File format '%s' not supported, only supports 'parquet' currently",
		                              data_file.file_format);
	}

	string file_path = path;
	if (options.allow_moved_paths) {
		auto iceberg_path = GetPath();
		auto &fs = FileSystem::GetFileSystem(context);
		file_path = IcebergUtils::GetFullPath(iceberg_path, path, fs);
	}
	OpenFileInfo res(file_path);
	auto extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	extended_info->options["file_size"] = Value::UBIGINT(data_file.file_size_in_bytes);
	res.extended_info = extended_info;
	return res;
}

bool IcebergMultiFileList::ManifestMatchesFilter(IcebergManifest &manifest) {
	auto spec_id = manifest.partition_spec_id;
	auto partition_spec_it = metadata->partition_specs.find(spec_id);
	if (partition_spec_it == metadata->partition_specs.end()) {
		throw InvalidInputException("Manifest %s references 'partition_spec_id' %d which doesn't exist",
		                            manifest.manifest_path, spec_id);
	}
	auto &partition_spec = partition_spec_it->second;
	if (partition_spec.fields.size() != manifest.field_summary.size()) {
		throw InvalidInputException(
		    "Manifest has %d 'field_summary' entries but the referenced partition spec has %d fields",
		    manifest.field_summary.size(), partition_spec.fields.size());
	}

	if (table_filters.filters.empty()) {
		//! There are no filters
		return true;
	}

	auto &schema = snapshot.schema;
	unordered_map<uint64_t, idx_t> source_to_column_id;
	for (idx_t i = 0; i < schema.size(); i++) {
		auto &column = schema[i];
		source_to_column_id[static_cast<uint64_t>(column.id)] = i;
	}

	for (idx_t i = 0; i < manifest.field_summary.size(); i++) {
		auto &field_summary = manifest.field_summary[i];
		auto &field = partition_spec.fields[i];

		auto column_id = source_to_column_id.at(field.source_id);

		// Find if we have a filter for this source column
		auto filter_it = table_filters.filters.find(column_id);
		if (filter_it == table_filters.filters.end()) {
			continue;
		}
		// Skip if no bounds available
		if (field_summary.lower_bound.empty() || field_summary.upper_bound.empty()) {
			continue;
		}

		auto &column = schema[column_id];
		IcebergPredicateStats stats;
		auto result_type = field.transform.GetSerializedType(column.type);
		DeserializeBounds(field_summary.lower_bound, field_summary.upper_bound, column.name, result_type, stats);

		auto &filter = *filter_it->second;
		if (!IcebergPredicate::MatchBounds(filter, stats, field.transform)) {
			return false;
		}
	}
	return true;
}

void IcebergMultiFileList::InitializeFiles(lock_guard<mutex> &guard) {
	if (initialized) {
		return;
	}
	initialized = true;
	auto &manifest_producer = this->manifest_producer;

	//! Load the snapshot
	auto iceberg_path = GetPath();
	auto &fs = FileSystem::GetFileSystem(context);
	auto iceberg_meta_path = IcebergSnapshot::GetMetaDataPath(context, iceberg_path, fs, options);
	metadata = IcebergMetadata::Parse(iceberg_meta_path, fs, options.metadata_compression_codec);

	switch (options.snapshot_source) {
	case SnapshotSource::LATEST: {
		snapshot = IcebergSnapshot::GetLatestSnapshot(*metadata, options);
		break;
	}
	case SnapshotSource::FROM_ID: {
		snapshot = IcebergSnapshot::GetSnapshotById(*metadata, options.snapshot_id, options);
		break;
	}
	case SnapshotSource::FROM_TIMESTAMP: {
		snapshot = IcebergSnapshot::GetSnapshotByTimestamp(*metadata, options.snapshot_timestamp, options);
		break;
	}
	default:
		throw InternalException("SnapshotSource type not implemented");
	}

	//! Set up the manifest + manifest entry readers
	if (snapshot.iceberg_format_version == 1) {
		data_manifest_entry_reader = make_uniq<ManifestReader>(IcebergManifestEntryV1::PopulateNameMapping,
		                                                       IcebergManifestEntryV1::VerifySchema);
		delete_manifest_entry_reader = make_uniq<ManifestReader>(IcebergManifestEntryV1::PopulateNameMapping,
		                                                         IcebergManifestEntryV1::VerifySchema);
		delete_manifest_entry_reader->skip_deleted = true;
		data_manifest_entry_reader->skip_deleted = true;
		manifest_reader =
		    make_uniq<ManifestReader>(IcebergManifestV1::PopulateNameMapping, IcebergManifestV1::VerifySchema);

		manifest_producer = IcebergManifestV1::ProduceEntries;
		entry_producer = IcebergManifestEntryV1::ProduceEntries;
	} else if (snapshot.iceberg_format_version == 2) {
		data_manifest_entry_reader = make_uniq<ManifestReader>(IcebergManifestEntryV2::PopulateNameMapping,
		                                                       IcebergManifestEntryV2::VerifySchema);
		delete_manifest_entry_reader = make_uniq<ManifestReader>(IcebergManifestEntryV2::PopulateNameMapping,
		                                                         IcebergManifestEntryV2::VerifySchema);
		delete_manifest_entry_reader->skip_deleted = true;
		data_manifest_entry_reader->skip_deleted = true;
		manifest_reader =
		    make_uniq<ManifestReader>(IcebergManifestV2::PopulateNameMapping, IcebergManifestV2::VerifySchema);

		manifest_producer = IcebergManifestV2::ProduceEntries;
		entry_producer = IcebergManifestEntryV2::ProduceEntries;
	} else {
		throw InvalidInputException("Reading from Iceberg version %d is not supported yet",
		                            snapshot.iceberg_format_version);
	}

	if (snapshot.snapshot_id == DConstants::INVALID_INDEX) {
		// we are in an empty table
		current_data_manifest = data_manifests.begin();
		current_delete_manifest = delete_manifests.begin();
		return;
	}

	// Read the manifest list, we need all the manifests to determine if we've seen all deletes
	auto manifest_list_full_path = options.allow_moved_paths
	                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
	                                   : snapshot.manifest_list;

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
		if (!ManifestMatchesFilter(manifest)) {
			DUCKDB_LOG_INFO(context, "duckdb.Extensions.Iceberg",
			                "Iceberg Filter Pushdown, skipped 'manifest_file': '%s'", manifest.manifest_path);
			//! Skip this manifest
			continue;
		}

		if (manifest.content == IcebergManifestContentType::DATA) {
			data_manifests.push_back(std::move(manifest));
		} else {
			D_ASSERT(manifest.content == IcebergManifestContentType::DELETE);
			delete_manifests.push_back(std::move(manifest));
		}
	}

	current_data_manifest = data_manifests.begin();
	current_delete_manifest = delete_manifests.begin();
}

//! Multi File Reader

unique_ptr<MultiFileReader> IcebergMultiFileReader::CreateInstance(const TableFunction &table) {
	(void)table;
	return make_uniq<IcebergMultiFileReader>();
}

shared_ptr<MultiFileList> IcebergMultiFileReader::CreateFileList(ClientContext &context, const vector<string> &paths,
                                                                 FileGlobOptions options) {
	if (paths.size() != 1) {
		throw BinderException("'iceberg_scan' only supports single path as input");
	}
	auto storage_location = IcebergUtils::GetStorageLocation(context, paths[0]);
	return make_shared_ptr<IcebergMultiFileList>(context, storage_location, this->options);
}

bool IcebergMultiFileReader::Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                                  vector<string> &names, MultiFileReaderBindData &bind_data) {
	auto &iceberg_multi_file_list = dynamic_cast<IcebergMultiFileList &>(files);

	iceberg_multi_file_list.Bind(return_types, names);
	// FIXME: apply final transformation for 'file_row_number' ???

	auto &schema = iceberg_multi_file_list.snapshot.schema;
	auto &columns = bind_data.schema;
	for (auto &item : schema) {
		MultiFileColumnDefinition column(item.name, item.type);
		column.default_expression = make_uniq<ConstantExpression>(item.default_value);
		column.identifier = Value::INTEGER(item.id);

		columns.push_back(column);
	}
	bind_data.mapping = MultiFileColumnMappingMode::BY_FIELD_ID;
	return true;
}

void IcebergMultiFileReader::BindOptions(MultiFileOptions &options, MultiFileList &files,
                                         vector<LogicalType> &return_types, vector<string> &names,
                                         MultiFileReaderBindData &bind_data) {
	// Disable all other multifilereader options
	options.auto_detect_hive_partitioning = false;
	options.hive_partitioning = false;
	options.union_by_name = false;

	MultiFileReader::BindOptions(options, files, return_types, names, bind_data);
}

unique_ptr<MultiFileReaderGlobalState>
IcebergMultiFileReader::InitializeGlobalState(ClientContext &context, const MultiFileOptions &file_options,
                                              const MultiFileReaderBindData &bind_data, const MultiFileList &file_list,
                                              const vector<MultiFileColumnDefinition> &global_columns,
                                              const vector<ColumnIndex> &global_column_ids) {

	vector<LogicalType> extra_columns;
	auto res = make_uniq<IcebergMultiFileReaderGlobalState>(extra_columns, file_list);
	return std::move(res);
}

static void ApplyFieldMapping(MultiFileColumnDefinition &col, vector<IcebergFieldMapping> &mappings,
                              case_insensitive_map_t<idx_t> &fields,
                              optional_ptr<MultiFileColumnDefinition> parent = nullptr) {
	if (!col.identifier.IsNull()) {
		return;
	}

	auto name = col.name;
	if (parent && parent->type.id() == LogicalTypeId::MAP && StringUtil::CIEquals(name, "key_value")) {
		//! Deal with MAP, it has a 'key_value' child, which holds the 'key' + 'value' columns
		for (auto &child : col.children) {
			ApplyFieldMapping(child, mappings, fields, parent);
		}
		return;
	}
	if (parent && parent->type.id() == LogicalTypeId::LIST && StringUtil::CIEquals(name, "list")) {
		//! Deal with LIST, it has a 'element' child, which has the column for the underlying list data
		name = "element";
	}

	auto it = fields.find(name);
	if (it == fields.end()) {
		throw InvalidConfigurationException("Column '%s' does not have a field-id, and no field-mapping exists for it!",
		                                    name);
	}
	auto &mapping = mappings[it->second];

	if (mapping.field_id != NumericLimits<int32_t>::Maximum()) {
		col.identifier = Value::INTEGER(mapping.field_id);
	}

	for (auto &child : col.children) {
		ApplyFieldMapping(child, mappings, mapping.field_mapping_indexes, col);
	}
}

static Value TransformPartitionValueFromBlob(const string_t &blob, const LogicalType &type) {
	auto result = IcebergValue::DeserializeValue(blob, type);
	if (result.HasError()) {
		throw InvalidConfigurationException(result.GetError());
	}
	return result.GetValue();
}

template <class T>
static Value TransformPartitionValueTemplated(const Value &value, const LogicalType &type) {
	T val = value.GetValue<T>();
	string_t blob((const char *)&val, sizeof(T));
	return TransformPartitionValueFromBlob(blob, type);
}

static Value TransformPartitionValue(const Value &value, const LogicalType &type) {
	D_ASSERT(!value.type().IsNested());
	switch (value.type().InternalType()) {
	case PhysicalType::BOOL:
		return TransformPartitionValueTemplated<bool>(value, type);
	case PhysicalType::INT8:
		return TransformPartitionValueTemplated<int8_t>(value, type);
	case PhysicalType::INT16:
		return TransformPartitionValueTemplated<int16_t>(value, type);
	case PhysicalType::INT32:
		return TransformPartitionValueTemplated<int32_t>(value, type);
	case PhysicalType::INT64:
		return TransformPartitionValueTemplated<int64_t>(value, type);
	case PhysicalType::INT128:
		return TransformPartitionValueTemplated<hugeint_t>(value, type);
	case PhysicalType::FLOAT:
		return TransformPartitionValueTemplated<float>(value, type);
	case PhysicalType::DOUBLE:
		return TransformPartitionValueTemplated<double>(value, type);
	case PhysicalType::VARCHAR: {
		return TransformPartitionValueFromBlob(value.GetValueUnsafe<string_t>(), type);
	}
	default:
		throw NotImplementedException("TransformPartitionValue: Value: '%s' -> '%s'", value.ToString(),
		                              type.ToString());
	}
}

static void ApplyPartitionConstants(const IcebergMultiFileList &multi_file_list, MultiFileReaderData &reader_data,
                                    const vector<MultiFileColumnDefinition> &global_columns,
                                    const vector<ColumnIndex> &global_column_ids) {
	// Get the metadata for this file
	auto &reader = *reader_data.reader;
	auto file_id = reader.file_list_idx.GetIndex();
	auto &data_file = multi_file_list.data_files[file_id];

	// Get the partition spec for this file
	auto &partition_specs = multi_file_list.metadata->partition_specs;
	auto spec_id = data_file.partition_spec_id;
	auto partition_spec_it = partition_specs.find(spec_id);
	if (partition_spec_it == partition_specs.end()) {
		throw InvalidConfigurationException("'partition_spec_id' %d doesn't exist in the metadata", spec_id);
	}

	auto &partition_spec = partition_spec_it->second;
	if (partition_spec.fields.empty()) {
		return; // No partition fields, continue with normal mapping
	}

	unordered_map<uint64_t, idx_t> identifier_to_field_id;
	for (idx_t i = 0; i < partition_spec.fields.size(); i++) {
		auto &field = partition_spec.fields[i];
		identifier_to_field_id[field.source_id] = i;
	}

	auto &local_columns = reader.columns;
	unordered_map<uint64_t, idx_t> local_field_id_to_index;
	for (idx_t i = 0; i < local_columns.size(); i++) {
		auto &local_column = local_columns[i];
		auto field_identifier = local_column.identifier.GetValue<int32_t>();
		auto field_id = static_cast<uint64_t>(field_identifier);
		local_field_id_to_index[field_id] = i;
	}

	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto global_id = global_column_ids[i];
		if (global_id.IsVirtualColumn()) {
			continue;
		}
		auto &global_column = global_columns[global_id.GetPrimaryIndex()];
		auto field_id = static_cast<uint64_t>(global_column.identifier.GetValue<int32_t>());
		if (local_field_id_to_index.count(field_id)) {
			//! Column exists in the local columns of the file
			continue;
		}

		auto it = identifier_to_field_id.find(field_id);
		if (it == identifier_to_field_id.end()) {
			continue;
		}

		auto &field = partition_spec.fields[it->second];
		if (field.transform != IcebergTransformType::IDENTITY) {
			continue; // Skip non-identity transforms
		}

		// Get the partition value from the data file's partition struct
		auto partition_value = data_file.partition;
		if (partition_value.IsNull()) {
			continue; // No partition value available
		}

		// Extract the field value from the partition struct
		const auto &struct_type = partition_value.type();
		const auto &struct_children = StructValue::GetChildren(partition_value);

		auto global_idx = MultiFileGlobalIndex(i);
		for (idx_t i = 0; i < StructType::GetChildCount(struct_type); i++) {
			auto &name = StructType::GetChildName(struct_type, i);
			if (name == field.name) {
				// Add to constant map - this will be used instead of reading from the file
				reader_data.constant_map.Add(global_idx,
				                             TransformPartitionValue(struct_children[i], global_column.type));
				break;
			}
		}
	}
}

void IcebergMultiFileReader::FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
                                          const MultiFileReaderBindData &options,
                                          const vector<MultiFileColumnDefinition> &global_columns,
                                          const vector<ColumnIndex> &global_column_ids, ClientContext &context,
                                          optional_ptr<MultiFileReaderGlobalState> global_state) {
	MultiFileReader::FinalizeBind(reader_data, file_options, options, global_columns, global_column_ids, context,
	                              global_state);
	D_ASSERT(global_state);
	// Get the metadata for this file
	const auto &multi_file_list = dynamic_cast<const IcebergMultiFileList &>(*global_state->file_list);
	{
		lock_guard<mutex> guard(multi_file_list.lock);
		D_ASSERT(multi_file_list.initialized);
	}
	auto &reader = *reader_data.reader;
	auto file_id = reader.file_list_idx.GetIndex();
	const auto &data_file = multi_file_list.data_files[file_id];

	// The path of the data file where this chunk was read from
	const auto &file_path = data_file.file_path;
	{
		std::lock_guard<mutex> guard(multi_file_list.delete_lock);
		if (multi_file_list.current_delete_manifest != multi_file_list.delete_manifests.end()) {
			multi_file_list.ProcessDeletes(global_columns);
		}
		reader.deletion_filter = std::move(multi_file_list.GetPositionalDeletesForFile(file_path));
	}

	auto &local_columns = reader_data.reader->columns;
	auto &mappings = multi_file_list.metadata->mappings;
	if (!multi_file_list.metadata->mappings.empty()) {
		auto &root = multi_file_list.metadata->mappings[0];
		for (auto &local_column : local_columns) {
			ApplyFieldMapping(local_column, mappings, root.field_mapping_indexes);
		}
	}
	ApplyPartitionConstants(multi_file_list, reader_data, global_columns, global_column_ids);
}

void IcebergMultiFileList::ScanPositionalDeleteFile(DataChunk &result) const {
	//! FIXME: might want to check the 'columns' of the 'reader' to check, field-ids are:
	auto names = FlatVector::GetData<string_t>(result.data[0]);  //! 2147483546
	auto row_ids = FlatVector::GetData<int64_t>(result.data[1]); //! 2147483545

	auto count = result.size();
	if (count == 0) {
		return;
	}
	reference<string_t> current_file_path = names[0];

	auto initial_key = current_file_path.get().GetString();
	auto it = positional_delete_data.find(initial_key);
	if (it == positional_delete_data.end()) {
		it = positional_delete_data.emplace(initial_key, make_uniq<IcebergPositionalDeleteData>()).first;
	}
	reference<IcebergPositionalDeleteData> deletes = *it->second;

	for (idx_t i = 0; i < count; i++) {
		auto &name = names[i];
		auto &row_id = row_ids[i];

		if (name != current_file_path.get()) {
			current_file_path = name;
			auto key = current_file_path.get().GetString();
			auto it = positional_delete_data.find(key);
			if (it == positional_delete_data.end()) {
				it = positional_delete_data.emplace(key, make_uniq<IcebergPositionalDeleteData>()).first;
			}
			deletes = *it->second;
		}

		deletes.get().AddRow(row_id);
	}
}

static void InitializeFromOtherChunk(DataChunk &target, DataChunk &other, const vector<column_t> &column_ids) {
	vector<LogicalType> types;
	for (auto &id : column_ids) {
		types.push_back(other.data[id].GetType());
	}
	target.InitializeEmpty(types);
}

void IcebergMultiFileList::ScanEqualityDeleteFile(const IcebergManifestEntry &entry, DataChunk &result_p,
                                                  vector<MultiFileColumnDefinition> &local_columns,
                                                  const vector<MultiFileColumnDefinition> &global_columns) const {
	D_ASSERT(!entry.equality_ids.empty());
	D_ASSERT(result_p.ColumnCount() == local_columns.size());

	auto count = result_p.size();
	if (count == 0) {
		return;
	}

	//! Map from column_id to 'local_columns' index, to figure out which columns from the 'result_p' are relevant here
	unordered_map<int32_t, column_t> id_to_column;
	for (column_t i = 0; i < local_columns.size(); i++) {
		auto &col = local_columns[i];
		D_ASSERT(!col.identifier.IsNull());
		id_to_column[col.identifier.GetValue<int32_t>()] = i;
	}

	vector<column_t> column_ids;
	DataChunk result;
	for (auto id : entry.equality_ids) {
		D_ASSERT(id_to_column.count(id));
		column_ids.push_back(id_to_column[id]);
	}

	//! Get or create the equality delete data for this sequence number
	auto it = equality_delete_data.find(entry.sequence_number);
	if (it == equality_delete_data.end()) {
		it = equality_delete_data
		         .emplace(entry.sequence_number, make_uniq<IcebergEqualityDeleteData>(entry.sequence_number))
		         .first;
	}
	auto &deletes = *it->second;

	//! Map from column_id to 'global_columns' index, so we can create a reference to the correct global index
	unordered_map<int32_t, column_t> id_to_global_column;
	for (column_t i = 0; i < global_columns.size(); i++) {
		auto &col = global_columns[i];
		D_ASSERT(!col.identifier.IsNull());
		id_to_global_column[col.identifier.GetValue<int32_t>()] = i;
	}

	//! Take only the relevant columns from the result
	InitializeFromOtherChunk(result, result_p, column_ids);
	result.ReferenceColumns(result_p, column_ids);
	deletes.files.emplace_back(entry.partition, entry.partition_spec_id);
	auto &rows = deletes.files.back().rows;
	rows.resize(count);
	D_ASSERT(result.ColumnCount() == entry.equality_ids.size());
	for (idx_t col_idx = 0; col_idx < result.ColumnCount(); col_idx++) {
		auto &field_id = entry.equality_ids[col_idx];
		auto global_column_id = id_to_global_column[field_id];
		auto &col = global_columns[global_column_id];
		auto &vec = result.data[col_idx];

		for (idx_t i = 0; i < count; i++) {
			auto &row = rows[i];
			auto constant = vec.GetValue(i);
			unique_ptr<Expression> equality_filter;
			auto bound_ref = make_uniq<BoundReferenceExpression>(col.type, global_column_id);
			if (!constant.IsNull()) {
				//! Create a COMPARE_NOT_EQUAL expression
				equality_filter =
				    make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_NOTEQUAL, std::move(bound_ref),
				                                         make_uniq<BoundConstantExpression>(constant));
			} else {
				//! Construct an OPERATOR_IS_NOT_NULL expression instead
				auto is_not_null =
				    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
				is_not_null->children.push_back(std::move(bound_ref));
				equality_filter = std::move(is_not_null);
			}
			row.filters.emplace(std::make_pair(field_id, std::move(equality_filter)));
		}
	}
}

void IcebergMultiFileList::ScanDeleteFile(const IcebergManifestEntry &entry,
                                          const vector<MultiFileColumnDefinition> &global_columns) const {
	const auto &delete_file_path = entry.file_path;
	auto &instance = DatabaseInstance::GetDatabase(context);
	//! FIXME: delete files could also be made without row_ids,
	//! in which case we need to rely on the `'schema.column-mapping.default'` property just like data files do.
	auto &parquet_scan_entry = ExtensionUtil::GetTableFunction(instance, "parquet_scan");
	auto &parquet_scan = parquet_scan_entry.functions.functions[0];

	// Prepare the inputs for the bind
	vector<Value> children;
	children.reserve(1);
	children.push_back(Value(delete_file_path));
	named_parameter_map_t named_params;
	vector<LogicalType> input_types;
	vector<string> input_names;

	TableFunctionRef empty;
	TableFunction dummy_table_function;
	dummy_table_function.name = "IcebergDeleteScan";
	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  dummy_table_function, empty);
	vector<LogicalType> return_types;
	vector<string> return_names;

	auto bind_data = parquet_scan.bind(context, bind_input, return_types, return_names);

	DataChunk result;
	// Reserve for STANDARD_VECTOR_SIZE instead of count, in case the returned table contains too many tuples
	result.Initialize(context, return_types, STANDARD_VECTOR_SIZE);

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);

	vector<column_t> column_ids;
	for (idx_t i = 0; i < return_types.size(); i++) {
		column_ids.push_back(i);
	}
	TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	auto global_state = parquet_scan.init_global(context, input);
	auto local_state = parquet_scan.init_local(execution_context, input, global_state.get());

	auto &multi_file_local_state = local_state->Cast<MultiFileLocalState>();

	if (entry.content == IcebergManifestEntryContentType::POSITION_DELETES) {
		do {
			TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
			result.Reset();
			parquet_scan.function(context, function_input, result);
			result.Flatten();
			ScanPositionalDeleteFile(result);
		} while (result.size() != 0);
	} else if (entry.content == IcebergManifestEntryContentType::EQUALITY_DELETES) {
		do {
			TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
			result.Reset();
			parquet_scan.function(context, function_input, result);
			result.Flatten();
			ScanEqualityDeleteFile(entry, result, multi_file_local_state.reader->columns, global_columns);
		} while (result.size() != 0);
	}
}

unique_ptr<IcebergPositionalDeleteData>
IcebergMultiFileList::GetPositionalDeletesForFile(const string &file_path) const {
	auto it = positional_delete_data.find(file_path);
	if (it != positional_delete_data.end()) {
		// There is delete data for this file, return it
		return std::move(it->second);
	}
	return nullptr;
}

void IcebergMultiFileList::ProcessDeletes(const vector<MultiFileColumnDefinition> &global_columns) const {
	// In <=v2 we now have to process *all* delete manifests
	// before we can be certain that we have all the delete data for the current file.

	// v3 solves this, `referenced_data_file` will tell us which file the `data_file`
	// is targeting before we open it, and there can only be one deletion vector per data file.

	// From the spec: "At most one deletion vector is allowed per data file in a snapshot"

	auto iceberg_path = GetPath();
	auto &fs = FileSystem::GetFileSystem(context);
	auto &entry_producer = this->entry_producer;

	vector<IcebergManifestEntry> delete_files;
	while (current_delete_manifest != delete_manifests.end()) {
		if (delete_manifest_entry_reader->Finished()) {
			if (current_delete_manifest == delete_manifests.end()) {
				break;
			}
			auto &manifest = *current_delete_manifest;
			auto manifest_entry_full_path = options.allow_moved_paths
			                                    ? IcebergUtils::GetFullPath(iceberg_path, manifest.manifest_path, fs)
			                                    : manifest.manifest_path;
			auto scan = make_uniq<AvroScan>("IcebergManifest", context, manifest_entry_full_path);
			delete_manifest_entry_reader->Initialize(std::move(scan));
			delete_manifest_entry_reader->SetSequenceNumber(manifest.sequence_number);
			delete_manifest_entry_reader->SetPartitionSpecID(manifest.partition_spec_id);
		}

		delete_manifest_entry_reader->ReadEntries(
		    STANDARD_VECTOR_SIZE, [&delete_files, &entry_producer](DataChunk &chunk, idx_t offset, idx_t count,
		                                                           const ManifestReaderInput &input) {
			    return entry_producer(chunk, offset, count, input, delete_files);
		    });

		if (delete_manifest_entry_reader->Finished()) {
			current_delete_manifest++;
			continue;
		}
	}

#ifdef DEBUG
	for (auto &entry : data_files) {
		D_ASSERT(entry.content == IcebergManifestEntryContentType::DATA);
		D_ASSERT(entry.status != IcebergManifestEntryStatusType::DELETED);
	}
#endif

	for (auto &entry : delete_files) {
		if (!StringUtil::CIEquals(entry.file_format, "parquet")) {
			throw NotImplementedException(
			    "File format '%s' not supported for deletes, only supports 'parquet' currently", entry.file_format);
		}
		ScanDeleteFile(entry, global_columns);
	}

	D_ASSERT(current_delete_manifest == delete_manifests.end());
}

void IcebergMultiFileReader::ApplyEqualityDeletes(ClientContext &context, DataChunk &output_chunk,
                                                  const IcebergMultiFileList &multi_file_list,
                                                  const IcebergManifestEntry &data_file,
                                                  const vector<MultiFileColumnDefinition> &local_columns) {
	vector<reference<IcebergEqualityDeleteRow>> delete_rows;

	auto &metadata = *multi_file_list.metadata;
	auto delete_data_it = multi_file_list.equality_delete_data.upper_bound(data_file.sequence_number);
	//! Look through all the equality delete files with a *higher* sequence number
	for (; delete_data_it != multi_file_list.equality_delete_data.end(); delete_data_it++) {
		auto &files = delete_data_it->second->files;
		for (auto &file : files) {
			auto &partition_spec = metadata.partition_specs.at(file.partition_spec_id);
			if (partition_spec.IsPartitioned()) {
				if (file.partition_spec_id != data_file.partition_spec_id) {
					//! Not unpartitioned and the data does not share the same partition spec as the delete, skip the
					//! delete file.
					continue;
				}
				if (file.partition != data_file.partition) {
					//! Same partition spec id, but the partitioning information doesn't match, delete file doesn't
					//! apply.
					continue;
				}
			}
			delete_rows.insert(delete_rows.end(), file.rows.begin(), file.rows.end());
		}
	}

	if (delete_rows.empty()) {
		return;
	}

	//! Map from column_id to 'local_columns' index
	unordered_map<int32_t, column_t> id_to_local_column;
	for (column_t i = 0; i < local_columns.size(); i++) {
		auto &col = local_columns[i];
		D_ASSERT(!col.identifier.IsNull());
		id_to_local_column[col.identifier.GetValue<int32_t>()] = i;
	}

	//! Create a big CONJUNCTION_AND of all the rows, illustrative example:
	//! WHERE
	//!	(col1 != 'A' OR col2 != 'B') AND
	//!	(col1 != 'C' OR col2 != 'D') AND
	//!	(col1 != 'X' OR col2 != 'Y') AND
	//!	(col1 != 'Z' OR col2 != 'W')

	vector<unique_ptr<Expression>> rows;
	for (auto &row : delete_rows) {
		vector<unique_ptr<Expression>> equalities;
		for (auto &item : row.get().filters) {
			auto &field_id = item.first;
			auto &expression = item.second;

			bool treat_as_null = !id_to_local_column.count(field_id);
			if (treat_as_null) {
				//! This column is not present in the file
				//! For the purpose of the equality deletes, we are treating it as if its value is NULL (despite any
				//! 'initial-default' that exists)

				//! This means that if the expression is 'IS_NOT_NULL', the result is False for this column, otherwise
				//! it's True (because nothing compares equal to NULL)
				if (expression->type == ExpressionType::OPERATOR_IS_NOT_NULL) {
					equalities.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(false)));
				} else {
					equalities.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
				}
			} else {
				equalities.push_back(expression->Copy());
			}
		}

		unique_ptr<Expression> filter;
		D_ASSERT(!equalities.empty());
		if (equalities.size() > 1) {
			auto conjunction_or = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR);
			conjunction_or->children = std::move(equalities);
			filter = std::move(conjunction_or);
		} else {
			filter = std::move(equalities[0]);
		}
		rows.push_back(std::move(filter));
	}

	unique_ptr<Expression> equality_delete_filter;
	D_ASSERT(!rows.empty());
	if (rows.size() == 1) {
		equality_delete_filter = std::move(rows[0]);
	} else {
		auto conjunction_and = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		conjunction_and->children = std::move(rows);
		equality_delete_filter = std::move(conjunction_and);
	}

	//! Apply equality deletes
	ExpressionExecutor expression_executor(context);
	expression_executor.AddExpression(*equality_delete_filter);
	SelectionVector sel_vec(STANDARD_VECTOR_SIZE);
	idx_t count = expression_executor.SelectExpression(output_chunk, sel_vec);
	output_chunk.Slice(sel_vec, count);
}

void IcebergMultiFileReader::FinalizeChunk(ClientContext &context, const MultiFileBindData &bind_data,
                                           BaseFileReader &reader, const MultiFileReaderData &reader_data,
                                           DataChunk &input_chunk, DataChunk &output_chunk,
                                           ExpressionExecutor &executor,
                                           optional_ptr<MultiFileReaderGlobalState> global_state) {
	// Base class finalization first
	MultiFileReader::FinalizeChunk(context, bind_data, reader, reader_data, input_chunk, output_chunk, executor,
	                               global_state);

	D_ASSERT(global_state);
	// Get the metadata for this file
	const auto &multi_file_list = dynamic_cast<const IcebergMultiFileList &>(*global_state->file_list);
	auto file_id = reader.file_list_idx.GetIndex();
	auto &data_file = multi_file_list.data_files[file_id];
	auto &local_columns = reader.columns;

	ApplyEqualityDeletes(context, output_chunk, multi_file_list, data_file, local_columns);
}

bool IcebergMultiFileReader::ParseOption(const string &key, const Value &val, MultiFileOptions &options,
                                         ClientContext &context) {
	auto loption = StringUtil::Lower(key);
	if (loption == "allow_moved_paths") {
		this->options.allow_moved_paths = BooleanValue::Get(val);
		return true;
	}
	if (loption == "metadata_compression_codec") {
		this->options.metadata_compression_codec = StringValue::Get(val);
		return true;
	}
	if (loption == "skip_schema_inference") {
		this->options.infer_schema = !BooleanValue::Get(val);
		return true;
	}
	if (loption == "version") {
		this->options.table_version = StringValue::Get(val);
		return true;
	}
	if (loption == "version_name_format") {
		auto value = StringValue::Get(val);
		auto string_substitutions = IcebergUtils::CountOccurrences(value, "%s");
		if (string_substitutions != 2) {
			throw InvalidInputException("'version_name_format' has to contain two occurrences of '%s' in it, found %d",
			                            "%s", string_substitutions);
		}
		this->options.version_name_format = value;
		return true;
	}
	if (loption == "snapshot_from_id") {
		if (this->options.snapshot_source != SnapshotSource::LATEST) {
			throw InvalidInputException("Can't use 'snapshot_from_id' in combination with 'snapshot_from_timestamp'");
		}
		this->options.snapshot_source = SnapshotSource::FROM_ID;
		this->options.snapshot_id = val.GetValue<uint64_t>();
		return true;
	}
	if (loption == "snapshot_from_timestamp") {
		if (this->options.snapshot_source != SnapshotSource::LATEST) {
			throw InvalidInputException("Can't use 'snapshot_from_id' in combination with 'snapshot_from_timestamp'");
		}
		this->options.snapshot_source = SnapshotSource::FROM_TIMESTAMP;
		this->options.snapshot_timestamp = val.GetValue<timestamp_t>();
		return true;
	}
	return MultiFileReader::ParseOption(key, val, options, context);
}

} // namespace duckdb
