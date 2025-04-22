#include "iceberg_multi_file_reader.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/main/extension_helper.hpp"

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
                                                                      const MultiFileOptions &options,
                                                                      MultiFilePushdownInfo &info,
                                                                      vector<unique_ptr<Expression>> &filters) {
	//! FIXME: We don't handle filter pushdown yet into the file list
	//! Leaving the skeleton here because we want to add this relatively soon anyways
	return nullptr;
}

vector<OpenFileInfo> IcebergMultiFileList::GetAllFiles() {
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
	while (!GetFile(i).path.empty()) {
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

OpenFileInfo IcebergMultiFileList::GetFile(idx_t file_id) {
	if (!initialized) {
		InitializeFiles();
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
		}

		idx_t remaining = (file_id + 1) - data_files.size();
		data_manifest_entry_reader->ReadEntries(
		    remaining, [&data_files, &entry_producer](DataChunk &chunk, idx_t offset, idx_t count,
		                                              const ManifestReaderInput &input) {
			    return entry_producer(chunk, offset, count, input, data_files);
		    });
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
	auto &data_file = data_files[file_id];
	auto &path = data_file.file_path;

	string file_path = path;
	if (options.allow_moved_paths) {
		auto iceberg_path = GetPath();
		auto &fs = FileSystem::GetFileSystem(context);
		file_path = IcebergUtils::GetFullPath(iceberg_path, path, fs);
	}
	return OpenFileInfo(file_path);
}

void IcebergMultiFileList::InitializeFiles() {
	lock_guard<mutex> guard(lock);
	if (initialized) {
		return;
	}
	initialized = true;
	auto &manifest_producer = this->manifest_producer;

	//! Load the snapshot
	auto iceberg_path = GetPath();
	auto &fs = FileSystem::GetFileSystem(context);
	auto iceberg_meta_path = IcebergSnapshot::GetMetaDataPath(context, iceberg_path, fs, options);
	auto metadata = IcebergMetadata::Parse(iceberg_meta_path, fs, options.metadata_compression_codec);

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
	return make_shared_ptr<IcebergMultiFileList>(context, paths[0], this->options);
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
	//! FIXME: check if 'schema.name-mapping.default' is set, act on it to support "column-mapping"
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
	auto &reader = *reader_data.reader;
	auto file_id = reader.file_list_idx.GetIndex();
	auto &data_file = multi_file_list.data_files[file_id];

	// The path of the data file where this chunk was read from
	auto &file_path = data_file.file_path;
	{
		std::lock_guard<mutex> guard(multi_file_list.delete_lock);
		if (multi_file_list.current_delete_manifest != multi_file_list.delete_manifests.end()) {
			multi_file_list.ProcessDeletes();
		}
		reader.deletion_filter = multi_file_list.GetDeletesForFile(file_path);
	}
}

void IcebergMultiFileList::ScanDeleteFile(const string &delete_file_path) const {
	auto &instance = DatabaseInstance::GetDatabase(context);

	//! Make sure parquet is loaded, throws if it can't be loaded.
	ExtensionHelper::AutoLoadExtension(instance, "parquet");

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

		auto initial_key = current_file_path.get().GetString();
		auto it = delete_data.find(initial_key);
		if (it == delete_data.end()) {
			it = delete_data.emplace(initial_key, make_uniq<IcebergDeleteData>()).first;
		}
		reference<IcebergDeleteData> deletes = *it->second;

		for (idx_t i = 0; i < count; i++) {
			auto &name = names[i];
			auto &row_id = row_ids[i];

			if (name != current_file_path.get()) {
				current_file_path = name;
				auto key = current_file_path.get().GetString();
				auto it = delete_data.find(key);
				if (it == delete_data.end()) {
					it = delete_data.emplace(key, make_uniq<IcebergDeleteData>()).first;
				}
				deletes = *it->second;
			}

			deletes.get().AddRow(row_id);
		}
	} while (result.size() != 0);
}

unique_ptr<IcebergDeleteData> IcebergMultiFileList::GetDeletesForFile(const string &file_path) const {
	auto it = delete_data.find(file_path);
	if (it != delete_data.end()) {
		// There is delete data for this file, return it
		return std::move(it->second);
	}
	return nullptr;
}

void IcebergMultiFileList::ProcessDeletes() const {
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
		ScanDeleteFile(entry.file_path);
	}

	D_ASSERT(current_delete_manifest == delete_manifests.end());
}

void IcebergMultiFileReader::FinalizeChunk(ClientContext &context, const MultiFileBindData &bind_data,
                                           BaseFileReader &reader, const MultiFileReaderData &reader_data,
                                           DataChunk &input_chunk, DataChunk &output_chunk,
                                           ExpressionExecutor &executor,
                                           optional_ptr<MultiFileReaderGlobalState> global_state) {
	// Base class finalization first
	MultiFileReader::FinalizeChunk(context, bind_data, reader, reader_data, input_chunk, output_chunk, executor,
	                               global_state);
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
		this->options.skip_schema_inference = BooleanValue::Get(val);
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
