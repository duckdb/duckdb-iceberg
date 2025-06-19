#include "iceberg_multi_file_reader.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_logging.hpp"
#include "iceberg_predicate.hpp"
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

#include "metadata/iceberg_predicate_stats.hpp"
#include "metadata/iceberg_table_metadata.hpp"

namespace duckdb {

IcebergMultiFileList::IcebergMultiFileList(ClientContext &context_p, shared_ptr<IcebergScanInfo> scan_info,
                                           const string &path, const IcebergOptions &options)
    : MultiFileList(vector<OpenFileInfo> {}, FileGlobOptions::ALLOW_EMPTY), context(context_p),
      fs(FileSystem::GetFileSystem(context)), scan_info(scan_info), path(path), lock(), options(options) {
}

string IcebergMultiFileList::ToDuckDBPath(const string &raw_path) {
	return raw_path;
}

string IcebergMultiFileList::GetPath() const {
	return path;
}

const IcebergTableMetadata &IcebergMultiFileList::GetMetadata() const {
	return scan_info->metadata;
}

optional_ptr<IcebergSnapshot> IcebergMultiFileList::GetSnapshot() const {
	return scan_info->snapshot;
}

const IcebergTableSchema &IcebergMultiFileList::GetSchema() const {
	return scan_info->schema;
}

void IcebergMultiFileList::Bind(vector<LogicalType> &return_types, vector<string> &names) {
	lock_guard<mutex> guard(lock);

	if (have_bound) {
		names = this->names;
		return_types = this->types;
		return;
	}

	if (!scan_info) {
		D_ASSERT(!path.empty());
		auto input_string = path;
		auto iceberg_path = IcebergUtils::GetStorageLocation(context, input_string);
		auto iceberg_meta_path = IcebergTableMetadata::GetMetaDataPath(context, iceberg_path, fs, options);
		auto table_metadata = IcebergTableMetadata::Parse(iceberg_meta_path, fs, options.metadata_compression_codec);

		auto temp_data = make_uniq<IcebergScanTemporaryData>();
		temp_data->metadata = IcebergTableMetadata::FromTableMetadata(table_metadata);
		auto &metadata = temp_data->metadata;

		auto found_snapshot = metadata.GetSnapshot(options.snapshot_lookup);
		shared_ptr<IcebergTableSchema> schema;
		if (options.snapshot_lookup.snapshot_source == SnapshotSource::LATEST) {
			schema = metadata.GetSchemaFromId(metadata.current_schema_id);
		} else {
			schema = metadata.GetSchemaFromId(found_snapshot->schema_id);
		}
		scan_info = make_shared_ptr<IcebergScanInfo>(iceberg_path, std::move(temp_data), found_snapshot, *schema);
	}

	if (!initialized) {
		InitializeFiles(guard);
	}

	auto &schema = GetSchema().columns;
	for (auto &schema_entry : schema) {
		names.push_back(schema_entry->name);
		return_types.push_back(schema_entry->type);
	}

	QueryResult::DeduplicateColumns(names);
	for (idx_t i = 0; i < names.size(); i++) {
		schema[i]->name = names[i];
	}

	have_bound = true;
	this->names = names;
	this->types = return_types;
}

unique_ptr<IcebergMultiFileList> IcebergMultiFileList::PushdownInternal(ClientContext &context,
                                                                        TableFilterSet &new_filters) const {
	auto filtered_list = make_uniq<IcebergMultiFileList>(context, scan_info, path, this->options);

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
	//! Lock is required because it reads the 'data_files' vector
	lock_guard<mutex> guard(lock);
	for (idx_t i = 0; i < data_files.size(); i++) {
		file_list.push_back(GetFileInternal(i, guard));
	}
	return file_list;
}

FileExpandResult IcebergMultiFileList::GetExpandResult() {
	// GetFileInternal(1) will ensure files with index 0 and index 1 are expanded if they are available
	lock_guard<mutex> guard(lock);
	GetFileInternal(1, guard);

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
	lock_guard<mutex> guard(lock);

	idx_t i = data_files.size();
	while (!GetFileInternal(i, guard).path.empty()) {
		i++;
	}
	return data_files.size();
}

unique_ptr<NodeStatistics> IcebergMultiFileList::GetCardinality(ClientContext &context) {
	idx_t cardinality = 0;

	if (GetMetadata().iceberg_version == 1) {
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

static void DeserializeBounds(const Value &lower_bound, const Value &upper_bound, const string &name,
                              const LogicalType &type, IcebergPredicateStats &out) {
	if (lower_bound.IsNull()) {
		out.lower_bound = Value(type);
	} else {
		D_ASSERT(lower_bound.type().id() == LogicalTypeId::BLOB);
		auto lower_bound_blob = lower_bound.GetValueUnsafe<string_t>();
		auto deserialized_lower_bound = IcebergValue::DeserializeValue(lower_bound_blob, type);
		if (deserialized_lower_bound.HasError()) {
			throw InvalidConfigurationException("Column %s lower bound deserialization failed: %s", name,
			                                    deserialized_lower_bound.GetError());
		}
		out.lower_bound = deserialized_lower_bound.GetValue();
	}

	if (upper_bound.IsNull()) {
		out.upper_bound = Value(type);
	} else {
		D_ASSERT(upper_bound.type().id() == LogicalTypeId::BLOB);
		auto upper_bound_blob = upper_bound.GetValueUnsafe<string_t>();
		auto deserialized_upper_bound = IcebergValue::DeserializeValue(upper_bound_blob, type);
		if (deserialized_upper_bound.HasError()) {
			throw InvalidConfigurationException("Column %s upper bound deserialization failed: %s", name,
			                                    deserialized_upper_bound.GetError());
		}
		out.upper_bound = deserialized_upper_bound.GetValue();
	}
}

bool IcebergMultiFileList::FileMatchesFilter(const IcebergManifestEntry &file) {
	D_ASSERT(!table_filters.filters.empty());

	auto &filters = table_filters.filters;
	auto &schema = GetSchema().columns;

	for (idx_t column_id = 0; column_id < schema.size(); column_id++) {
		// FIXME: is there a potential mismatch between column_id / field_id lurking here?
		auto &column = *schema[column_id];
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
		Value lower_bound;
		Value upper_bound;
		if (lower_bound_it != file.lower_bounds.end()) {
			lower_bound = lower_bound_it->second;
		}
		if (upper_bound_it != file.upper_bounds.end()) {
			upper_bound = upper_bound_it->second;
		}

		IcebergPredicateStats stats;
		DeserializeBounds(lower_bound, upper_bound, column.name, column.type, stats);
		auto null_counts_it = file.null_value_counts.find(source_id);
		if (null_counts_it != file.null_value_counts.end()) {
			auto &null_counts = null_counts_it->second;
			stats.has_null = null_counts != 0;
		}
		auto nan_counts_it = file.nan_value_counts.find(source_id);
		if (nan_counts_it != file.nan_value_counts.end()) {
			auto &nan_counts = nan_counts_it->second;
			stats.has_nan = nan_counts != 0;
		}

		auto &filter = *it->second;
		if (!IcebergPredicate::MatchBounds(filter, stats, IcebergTransform::Identity())) {
			//! If any predicate fails, exclude the file
			return false;
		}
	}
	return true;
}

optional_ptr<const IcebergManifestEntry> IcebergMultiFileList::GetDataFile(idx_t file_id, lock_guard<mutex> &guard) {
	if (file_id < data_files.size()) {
		//! Have we already scanned this data file and returned it? If so, return it
		return data_files[file_id];
	}

	while (file_id >= data_files.size()) {
		if (current_data_files.empty() || data_file_idx >= current_data_files.size()) {
			//! Load the next manifest file
			if (current_data_manifest == data_manifests.end()) {
				//! No more data manifests to explore
				return nullptr;
			}

			auto &manifest = *current_data_manifest;
			auto full_path = options.allow_moved_paths ? IcebergUtils::GetFullPath(path, manifest.manifest_path, fs)
			                                           : manifest.manifest_path;
			auto scan = make_uniq<AvroScan>("IcebergManifest", context, full_path);

			data_manifest_reader->Initialize(std::move(scan));
			data_manifest_reader->SetSequenceNumber(manifest.sequence_number);
			data_manifest_reader->SetPartitionSpecID(manifest.partition_spec_id);

			current_data_files.clear();
			while (!data_manifest_reader->Finished()) {
				data_manifest_reader->Read(STANDARD_VECTOR_SIZE, current_data_files);
			}

			current_data_manifest++;
			data_file_idx = 0;
		}

		optional_ptr<const IcebergManifestEntry> result;
		while (data_file_idx < current_data_files.size()) {
			auto &data_file = current_data_files[data_file_idx];
			if (!table_filters.filters.empty() && !FileMatchesFilter(data_file)) {
				DUCKDB_LOG(context, IcebergLogType, "Iceberg Filter Pushdown, skipped 'data_file': '%s'",
				           data_file.file_path);
				//! Skip this file
				data_file_idx++;
				continue;
			}

			result = data_file;
			data_file_idx++;
			break;
		}
		if (!result) {
			return nullptr;
		}

		data_files.push_back(*result);
	}

	return data_files[file_id];
}

OpenFileInfo IcebergMultiFileList::GetFileInternal(idx_t file_id, lock_guard<mutex> &guard) {

	if (!initialized) {
		InitializeFiles(guard);
	}

	if (!scan_info->snapshot) {
		return OpenFileInfo();
	}

	auto found_data_file = GetDataFile(file_id, guard);
	if (!found_data_file) {
		return OpenFileInfo();
	}

	const auto &data_file = *found_data_file;
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
	// files managed by Iceberg are never modified - we can keep them cached
	extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
	// etag / last modified time can be set to dummy values
	extended_info->options["etag"] = Value("");
	extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
	res.extended_info = extended_info;
	return res;
}

OpenFileInfo IcebergMultiFileList::GetFile(idx_t file_id) {
	lock_guard<mutex> guard(lock);
	return GetFileInternal(file_id, guard);
}

bool IcebergMultiFileList::ManifestMatchesFilter(const IcebergManifest &manifest) {
	auto spec_id = manifest.partition_spec_id;
	auto &metadata = GetMetadata();

	auto partition_spec_it = metadata.partition_specs.find(spec_id);
	if (partition_spec_it == metadata.partition_specs.end()) {
		throw InvalidInputException("Manifest %s references 'partition_spec_id' %d which doesn't exist",
		                            manifest.manifest_path, spec_id);
	}
	auto &partition_spec = partition_spec_it->second;
	if (!manifest.partitions.has_partitions) {
		//! No field summaries are present, can't filter anything
		return true;
	}

	auto &field_summaries = manifest.partitions.field_summary;
	if (partition_spec.fields.size() != field_summaries.size()) {
		throw InvalidInputException(
		    "Manifest has %d 'field_summary' entries but the referenced partition spec has %d fields",
		    field_summaries.size(), partition_spec.fields.size());
	}

	if (table_filters.filters.empty()) {
		//! There are no filters
		return true;
	}

	auto &schema = GetSchema().columns;
	unordered_map<uint64_t, idx_t> source_to_column_id;
	for (idx_t i = 0; i < schema.size(); i++) {
		auto &column = schema[i];
		source_to_column_id[static_cast<uint64_t>(column->id)] = i;
	}

	for (idx_t i = 0; i < field_summaries.size(); i++) {
		auto &field_summary = field_summaries[i];
		auto &field = partition_spec.fields[i];

		auto column_id = source_to_column_id.at(field.source_id);

		// Find if we have a filter for this source column
		auto filter_it = table_filters.filters.find(column_id);
		if (filter_it == table_filters.filters.end()) {
			continue;
		}

		auto &column = schema[column_id];
		IcebergPredicateStats stats;
		auto result_type = field.transform.GetSerializedType(column->type);
		DeserializeBounds(field_summary.lower_bound, field_summary.upper_bound, column->name, result_type, stats);
		stats.has_nan = field_summary.contains_nan;
		stats.has_null = field_summary.contains_null;

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
	if (!scan_info->snapshot) {
		// we are reading from an empty table
		current_data_manifest = data_manifests.begin();
		current_delete_manifest = delete_manifests.begin();
		return;
	}

	//! Load the snapshot
	auto iceberg_path = GetPath();
	auto &snapshot = *GetSnapshot();
	auto &metadata = GetMetadata();
	auto &fs = FileSystem::GetFileSystem(context);

	data_manifest_reader = make_uniq<manifest_file::ManifestFileReader>(metadata.iceberg_version);
	delete_manifest_reader = make_uniq<manifest_file::ManifestFileReader>(metadata.iceberg_version);

	// Read the manifest list, we need all the manifests to determine if we've seen all deletes
	auto manifest_list_full_path = options.allow_moved_paths
	                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
	                                   : snapshot.manifest_list;

	IcebergManifestList manifest_list(manifest_list_full_path);

	//! Read the manifest list
	auto manifest_list_reader = make_uniq<manifest_list::ManifestListReader>(metadata.iceberg_version);
	auto scan = make_uniq<AvroScan>("IcebergManifestList", context, manifest_list_full_path);
	manifest_list_reader->Initialize(std::move(scan));
	while (!manifest_list_reader->Finished()) {
		manifest_list_reader->Read(STANDARD_VECTOR_SIZE, manifest_list.manifests);
	}

	for (auto &manifest : manifest_list.manifests) {
		if (!ManifestMatchesFilter(manifest)) {
			DUCKDB_LOG(context, IcebergLogType, "Iceberg Filter Pushdown, skipped 'manifest_file': '%s'",
			           manifest.manifest_path);
			//! Skip this manifest
			continue;
		}

		if (manifest.content == IcebergManifestContentType::DATA) {
			data_manifests.push_back(manifest);
		} else {
			D_ASSERT(manifest.content == IcebergManifestContentType::DELETE);
			delete_manifests.push_back(manifest);
		}
	}

	current_data_manifest = data_manifests.begin();
	current_delete_manifest = delete_manifests.begin();
}

void IcebergMultiFileList::ProcessDeletes(const vector<MultiFileColumnDefinition> &global_columns,
                                          const vector<ColumnIndex> &column_indexes) const {
	// In <=v2 we now have to process *all* delete manifests
	// before we can be certain that we have all the delete data for the current file.

	// v3 solves this, `referenced_data_file` will tell us which file the `data_file`
	// is targeting before we open it, and there can only be one deletion vector per data file.

	// From the spec: "At most one deletion vector is allowed per data file in a snapshot"

	//! NOTE: The lock is required because we're reading from the 'data_files' vector
	lock_guard<mutex> guard(lock);
	auto iceberg_path = GetPath();
	auto &fs = FileSystem::GetFileSystem(context);

	while (current_delete_manifest != delete_manifests.end()) {
		auto &manifest = *current_delete_manifest;
		auto full_path = options.allow_moved_paths ? IcebergUtils::GetFullPath(iceberg_path, manifest.manifest_path, fs)
		                                           : manifest.manifest_path;
		auto scan = make_uniq<AvroScan>("IcebergManifest", context, full_path);

		delete_manifest_reader->Initialize(std::move(scan));
		delete_manifest_reader->SetSequenceNumber(manifest.sequence_number);
		delete_manifest_reader->SetPartitionSpecID(manifest.partition_spec_id);

		IcebergManifestFile manifest_file(full_path);
		while (!delete_manifest_reader->Finished()) {
			delete_manifest_reader->Read(STANDARD_VECTOR_SIZE, manifest_file.data_files);
		}

		current_delete_manifest++;

		for (auto &entry : manifest_file.data_files) {
			if (!StringUtil::CIEquals(entry.file_format, "parquet")) {
				throw NotImplementedException(
				    "File format '%s' not supported for deletes, only supports 'parquet' currently", entry.file_format);
			}
			ScanDeleteFile(entry, global_columns, column_indexes);
		}
	}

	D_ASSERT(current_delete_manifest == delete_manifests.end());
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
                                                  const vector<MultiFileColumnDefinition> &global_columns,
                                                  const vector<ColumnIndex> &column_indexes) const {
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

	unordered_map<idx_t, idx_t> global_id_to_result_id;
	for (idx_t i = 0; i < column_indexes.size(); i++) {
		auto &column_index = column_indexes[i];
		if (column_index.IsVirtualColumn()) {
			continue;
		}
		auto global_id = column_index.GetPrimaryIndex();
		global_id_to_result_id[global_id] = i;
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

		auto it = global_id_to_result_id.find(global_column_id);
		if (it == global_id_to_result_id.end()) {
			throw NotImplementedException("Equality deletes need the relevant columns to be selected");
		}
		global_column_id = it->second;

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
                                          const vector<MultiFileColumnDefinition> &global_columns,
                                          const vector<ColumnIndex> &column_indexes) const {
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
			ScanEqualityDeleteFile(entry, result, multi_file_local_state.reader->columns, global_columns,
			                       column_indexes);
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

} // namespace duckdb
