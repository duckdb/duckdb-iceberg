#include "iceberg_multi_file_reader.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

IcebergMultiFileList::IcebergMultiFileList(ClientContext &context_p, const string &path, const IcebergOptions &options)
    : MultiFileList({path}, FileGlobOptions::ALLOW_EMPTY), lock(), context(context_p), options(options) {
}

string IcebergMultiFileList::ToDuckDBPath(const string &raw_path) {
	return raw_path;
}

string IcebergMultiFileList::GetPath() {
	return GetPaths()[0];
}

void IcebergMultiFileList::Bind(vector<LogicalType> &return_types, vector<string> &names) {
	if (!initialized) {
		InitializeFiles();
	}

	auto &schema = snapshot.schema;
	for (auto &schema_entry : schema) {
		names.push_back(schema_entry.name);
		return_types.push_back(schema_entry.type);
	}
}

unique_ptr<MultiFileList> IcebergMultiFileList::ComplexFilterPushdown(ClientContext &context,
                                                                      const MultiFileReaderOptions &options,
                                                                      MultiFilePushdownInfo &info,
                                                                      vector<unique_ptr<Expression>> &filters) {
	if (filters.empty()) {
		return nullptr;
	}

	FilterCombiner combiner(context);
	for (const auto &filter : filters) {
		combiner.AddFilter(filter->Copy());
	}
	auto filterstmp = combiner.GenerateTableScanFilters(info.column_indexes);

	auto new_list = make_uniq<IcebergMultiFileList>(context, GetPath(), this->options);
	new_list->snapshot = this->snapshot;
	new_list->table_filters = std::move(filterstmp);
	return new_list;
}

vector<string> IcebergMultiFileList::GetAllFiles() {
	throw NotImplementedException("NOT IMPLEMENTED");
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
	while (!GetFile(i).empty()) {
		i++;
	}
	return data_files.size();
}

unique_ptr<NodeStatistics> IcebergMultiFileList::GetCardinality(ClientContext &context) {
	auto total_file_count = IcebergMultiFileList::GetTotalFileCount();

	if (total_file_count == 0) {
		return make_uniq<NodeStatistics>(0, 0);
	}

	// FIXME: visit metadata to get a cardinality count

	return nullptr;
}

string IcebergMultiFileList::GetFile(idx_t file_id) {
	if (!initialized) {
		InitializeFiles();
	}

	// Read enough data files
	while (file_id >= data_files.size()) {
		if (reader_state.finished) {
			if (current_data_manifest == data_manifests.end()) {
				break;
			}
			auto &manifest = *current_data_manifest;
			reader_state = ManifestEntryReaderState(*manifest);
		}

		auto new_entry = data_manifest_entry_reader->GetNext(reader_state);
		if (!new_entry) {
			D_ASSERT(reader_state.finished);
			current_data_manifest++;
			continue;
		}
		if (new_entry->status == IcebergManifestEntryStatusType::DELETED) {
			// Skip deleted files
			continue;
		}
		D_ASSERT(new_entry->content == IcebergManifestEntryContentType::DATA);
		data_files.push_back(std::move(*new_entry));
	}

	if (file_id >= data_files.size()) {
		return string();
	}

	D_ASSERT(file_id < data_files.size());
	auto &data_file = data_files[file_id];
	auto &path = data_file.file_path;

	if (options.allow_moved_paths) {
		auto iceberg_path = GetPath();
		auto &fs = FileSystem::GetFileSystem(context);
		return IcebergUtils::GetFullPath(iceberg_path, path, fs);
	} else {
		return path;
	}
}

void IcebergMultiFileList::InitializeFiles() {
	lock_guard<mutex> guard(lock);
	if (initialized) {
		return;
	}
	initialized = true;

	//! Load the snapshot
	auto iceberg_path = GetPath();
	auto &fs = FileSystem::GetFileSystem(context);
	auto iceberg_meta_path = IcebergSnapshot::GetMetaDataPath(context, iceberg_path, fs, options);
	switch (options.snapshot_source) {
	case SnapshotSource::LATEST: {
		snapshot = IcebergSnapshot::GetLatestSnapshot(iceberg_meta_path, fs, options);
		break;
	}
	case SnapshotSource::FROM_ID: {
		snapshot = IcebergSnapshot::GetSnapshotById(iceberg_meta_path, fs, options.snapshot_id, options);
		break;
	}
	case SnapshotSource::FROM_TIMESTAMP: {
		snapshot = IcebergSnapshot::GetSnapshotByTimestamp(iceberg_meta_path, fs, options.snapshot_timestamp, options);
		break;
	}
	default:
		throw InternalException("SnapshotSource type not implemented");
	}

	//! Set up the manifest + manifest entry readers
	if (snapshot.iceberg_format_version == 1) {
		data_manifest_entry_reader =
		    make_uniq<ManifestEntryReaderV1>(iceberg_path, snapshot.manifest_list, fs, options);
		delete_manifest_entry_reader =
		    make_uniq<ManifestEntryReaderV1>(iceberg_path, snapshot.manifest_list, fs, options);
		manifest_reader = make_uniq<ManifestReaderV1>(iceberg_path, snapshot.manifest_list, fs, options);
	} else if (snapshot.iceberg_format_version == 2) {
		data_manifest_entry_reader =
		    make_uniq<ManifestEntryReaderV2>(iceberg_path, snapshot.manifest_list, fs, options);
		delete_manifest_entry_reader =
		    make_uniq<ManifestEntryReaderV2>(iceberg_path, snapshot.manifest_list, fs, options);
		manifest_reader = make_uniq<ManifestReaderV2>(iceberg_path, snapshot.manifest_list, fs, options);
	} else {
		throw InvalidInputException("Reading from Iceberg version %d is not supported yet", snapshot.iceberg_format_version);
	}

	// Read the manifest list, we need all the manifests to determine if we've seen all deletes
	while (!manifest_reader->Finished()) {
		auto manifest = manifest_reader->GetNext();
		if (!manifest) {
			break;
		}
		if (manifest->content == IcebergManifestContentType::DATA) {
			data_manifests.push_back(std::move(manifest));
		} else {
			D_ASSERT(manifest->content == IcebergManifestContentType::DELETE);
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
	return make_shared_ptr<IcebergMultiFileList>(context, paths[0], this->options);
}

bool IcebergMultiFileReader::Bind(MultiFileReaderOptions &options, MultiFileList &files,
                                  vector<LogicalType> &return_types, vector<string> &names,
                                  MultiFileReaderBindData &bind_data) {
	auto &iceberg_multi_file_list = dynamic_cast<IcebergMultiFileList &>(files);

	iceberg_multi_file_list.Bind(return_types, names);
	// FIXME: apply final transformation for 'file_row_number' ???

	auto &schema = iceberg_multi_file_list.snapshot.schema;
	auto &columns = bind_data.schema;
	for (auto &item : schema) {
		MultiFileReaderColumnDefinition column(item.name, item.type);
		column.default_expression = make_uniq<ConstantExpression>(item.default_value);
		column.identifier = Value::INTEGER(item.id);

		columns.push_back(column);
	}
	bind_data.file_row_number_idx = names.size();
	bind_data.mapping = MultiFileReaderColumnMappingMode::BY_FIELD_ID;
	return true;
}

void IcebergMultiFileReader::BindOptions(MultiFileReaderOptions &options, MultiFileList &files,
                                         vector<LogicalType> &return_types, vector<string> &names,
                                         MultiFileReaderBindData &bind_data) {
	// Disable all other multifilereader options
	options.auto_detect_hive_partitioning = false;
	options.hive_partitioning = false;
	options.union_by_name = false;

	MultiFileReader::BindOptions(options, files, return_types, names, bind_data);
}

void IcebergMultiFileReader::CreateColumnMapping(const string &file_name,
                                                 const vector<MultiFileReaderColumnDefinition> &local_columns,
                                                 const vector<MultiFileReaderColumnDefinition> &global_columns,
                                                 const vector<ColumnIndex> &global_column_ids,
                                                 MultiFileReaderData &reader_data,
                                                 const MultiFileReaderBindData &bind_data, const string &initial_file,
                                                 optional_ptr<MultiFileReaderGlobalState> global_state_p) {

	D_ASSERT(bind_data.mapping == MultiFileReaderColumnMappingMode::BY_FIELD_ID);
	MultiFileReader::CreateColumnMappingByFieldId(file_name, local_columns, global_columns, global_column_ids,
	                                              reader_data, bind_data, initial_file, global_state_p);

	auto &global_state = global_state_p->Cast<IcebergMultiFileReaderGlobalState>();
    // Check if the file_row_number column is an "extra_column" which is not part of the projection
	if (!global_state.file_row_number_idx.IsValid()) {
		return;
	}
	auto file_row_number_idx = global_state.file_row_number_idx.GetIndex();
    if (file_row_number_idx >= global_column_ids.size()) {
        // Build the name map
        case_insensitive_map_t<idx_t> name_map;
        for (idx_t col_idx = 0; col_idx < local_columns.size(); col_idx++) {
            name_map[local_columns[col_idx].name] = col_idx;
        }

        // Lookup the required column in the local map
        auto entry = name_map.find("file_row_number");
        if (entry == name_map.end()) {
            throw IOException("Failed to find the file_row_number column");
        }

        // Register the column to be scanned from this file
        reader_data.column_ids.push_back(entry->second);
        reader_data.column_indexes.emplace_back(entry->second);
        reader_data.column_mapping.push_back(file_row_number_idx);
    }

    // This may have changed: update it
    reader_data.empty_columns = reader_data.column_ids.empty();

}

unique_ptr<MultiFileReaderGlobalState>
IcebergMultiFileReader::InitializeGlobalState(ClientContext &context, const MultiFileReaderOptions &file_options,
                                              const MultiFileReaderBindData &bind_data, const MultiFileList &file_list,
                                              const vector<MultiFileReaderColumnDefinition> &global_columns,
                                              const vector<ColumnIndex> &global_column_ids) {

	vector<LogicalType> extra_columns;
	// Map of column_name -> column_index
	vector<pair<string, idx_t>> mapped_columns;

	// TODO: only add file_row_number column if there are deletes
	case_insensitive_map_t<LogicalType> columns_to_map = {
	    {"file_row_number", LogicalType::BIGINT},
	};

	// Create a map of the columns that are in the projection
	// So we can detect that the projection already contains the 'extra_column' below
	case_insensitive_map_t<idx_t> selected_columns;
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto global_id = global_column_ids[i];
		if (global_id.IsRowIdColumn()) {
			continue;
		}

		auto &global_name = global_columns[global_id.GetPrimaryIndex()].name;
		selected_columns.insert({global_name, i});
	}

	// Map every column to either a column in the projection, or add it to the extra columns if it doesn't exist
	idx_t col_offset = 0;
	for (const auto &extra_column : columns_to_map) {
		// First check if the column is in the projection
		auto res = selected_columns.find(extra_column.first);
		if (res != selected_columns.end()) {
			// The column is in the projection, no special handling is required; we simply store the index
			mapped_columns.push_back({extra_column.first, res->second});
			continue;
		}

		// The column is NOT in the projection: it needs to be added as an extra_column

		// Calculate the index of the added column (extra columns are added after all other columns)
		idx_t current_col_idx = global_column_ids.size() + col_offset++;

		// Add column to the map, to ensure the MultiFileReader can find it when processing the Chunk
		mapped_columns.push_back({extra_column.first, current_col_idx});

		// Ensure the result DataChunk has a vector of the correct type to store this column
		extra_columns.push_back(extra_column.second);
	}

	auto res = make_uniq<IcebergMultiFileReaderGlobalState>(extra_columns, file_list);

	// Parse all the mapped columns into the DeltaMultiFileReaderGlobalState for easy use;
	for (const auto &mapped_column : mapped_columns) {
		auto &column_name = mapped_column.first;
		auto column_index = mapped_column.second;
		if (StringUtil::CIEquals(column_name, "file_row_number")) {
			if (res->file_row_number_idx.IsValid()) {
				throw InvalidInputException("'file_row_number' already set!");
			}
			res->file_row_number_idx = column_index;
		} else {
			throw InternalException("Extra column type not handled");
		}
	}
	return std::move(res);
}

void IcebergMultiFileReader::FinalizeBind(const MultiFileReaderOptions &file_options,
	                                     const MultiFileReaderBindData &options, const string &filename,
	                                     const vector<MultiFileReaderColumnDefinition> &local_columns,
	                                     const vector<MultiFileReaderColumnDefinition> &global_columns,
	                                     const vector<ColumnIndex> &global_column_ids, MultiFileReaderData &reader_data,
	                                     ClientContext &context, optional_ptr<MultiFileReaderGlobalState> global_state) {
	MultiFileReader::FinalizeBind(file_options, options, filename, local_columns, global_columns,
	                              global_column_ids, reader_data, context, global_state);
	return;
}

void IcebergMultiFileList::ScanDeleteFile(const string &delete_file_path) const {
	auto &instance = DatabaseInstance::GetDatabase(context);
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

	do {
		TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
		result.Reset();
		parquet_scan.function(context, function_input, result);

		idx_t count = result.size();
		for (auto &vec : result.data) {
			vec.Flatten(count);
		}

		auto names = FlatVector::GetData<string_t>(result.data[0]);
		auto row_ids = FlatVector::GetData<int64_t>(result.data[1]);

		if (count == 0) {
			continue;
		}
		reference<string_t> current_file_path = names[0];
		reference<IcebergDeleteData> deletes = delete_data[current_file_path.get().GetString()];

		for (idx_t i = 0; i < count; i++) {
			auto &name = names[i];
			auto &row_id = row_ids[i];

			if (name != current_file_path.get()) {
				current_file_path = name;
				deletes = delete_data[current_file_path.get().GetString()];
			}

			deletes.get().AddRow(row_id);
		}
	} while (result.size() != 0);
}

optional_ptr<IcebergDeleteData> IcebergMultiFileList::GetDeletesForFile(const string &file_path) const {
	auto it = delete_data.find(file_path);
	if (it != delete_data.end()) {
		// There is delete data for this file, return it
		auto &deletes = it->second;
		return deletes;
	}
	return nullptr;
}

void IcebergMultiFileList::ProcessDeletes() const {
	// In <=v2 we now have to process *all* delete manifests
	// before we can be certain that we have all the delete data for the current file.

	// v3 solves this, `referenced_data_file` will tell us which file the `data_file`
	// is targeting before we open it, and there can only be one deletion vector per data file.

	// From the spec: "At most one deletion vector is allowed per data file in a snapshot"

	ManifestEntryReaderState reader_state;
	while (current_delete_manifest != delete_manifests.end()) {
		if (reader_state.finished) {
			auto &manifest = *current_delete_manifest;
			reader_state = ManifestEntryReaderState(*manifest);
		}

		auto new_entry = delete_manifest_entry_reader->GetNext(reader_state);
		if (!new_entry) {
			D_ASSERT(reader_state.finished);
			current_delete_manifest++;
			continue;
		}
		if (new_entry->status == IcebergManifestEntryStatusType::DELETED) {
			// Skip deleted files
			continue;
		}
		D_ASSERT(new_entry->content != IcebergManifestEntryContentType::DATA);
		//! FIXME: with v3 we can check from the metadata whether this targets our file
		// we can avoid (read: delay) materializing the file in that case
		ScanDeleteFile(new_entry->file_path);
	}

	D_ASSERT(current_delete_manifest == delete_manifests.end());
}

void IcebergMultiFileReader::FinalizeChunk(ClientContext &context, const MultiFileReaderBindData &bind_data,
                                           const MultiFileReaderData &reader_data, DataChunk &chunk,
                                           optional_ptr<MultiFileReaderGlobalState> global_state) {
	// Base class finalization first
	MultiFileReader::FinalizeChunk(context, bind_data, reader_data, chunk, global_state);

	D_ASSERT(global_state);
	auto &iceberg_global_state = global_state->Cast<IcebergMultiFileReaderGlobalState>();
	D_ASSERT(iceberg_global_state.file_list);

	// Get the metadata for this file
	const auto &multi_file_list = dynamic_cast<const IcebergMultiFileList &>(*global_state->file_list);
	auto file_id = reader_data.file_list_idx.GetIndex();
	auto &data_file = multi_file_list.data_files[file_id];

	// The path of the data file where this chunk was read from
	auto &file_path = data_file.file_path;
	optional_ptr<IcebergDeleteData> delete_data;
	{
		std::lock_guard<mutex> guard(multi_file_list.delete_lock);
		if (multi_file_list.current_delete_manifest != multi_file_list.delete_manifests.end()) {
			multi_file_list.ProcessDeletes();
		}
		delete_data = multi_file_list.GetDeletesForFile(file_path);
	}

	//! FIXME: how can we retrieve which rows these were in the file?
	// Looks like delta does this by adding an extra projection so the chunk has a file_row_id column
	if (delete_data) {
		D_ASSERT(iceberg_global_state.file_row_number_idx.IsValid());
		auto &file_row_number_column = chunk.data[iceberg_global_state.file_row_number_idx.GetIndex()];

		delete_data->Apply(chunk, file_row_number_column);
	}
}

bool IcebergMultiFileReader::ParseOption(const string &key, const Value &val, MultiFileReaderOptions &options,
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
		this->options.skip_schema_inference = BooleanValue::Get(val);
		return true;
	}
	if (loption == "version") {
		this->options.table_version = StringValue::Get(val);
		return true;
	}
	if (loption == "version_name_format") {
		this->options.version_name_format = StringValue::Get(val);
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
