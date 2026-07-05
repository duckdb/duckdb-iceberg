#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system_wrapper.hpp"

#include "function/iceberg_functions.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "iceberg_options.hpp"

namespace duckdb {

namespace {

struct IcebergTablePartitionStatsFile {
	int64_t snapshot_id;
	string statistics_path;
	int64_t file_size_in_bytes;
};

struct NestedScanState {
	TableFunction function;
	unique_ptr<FunctionData> bind_data;
	unique_ptr<GlobalTableFunctionState> global_state;
	unique_ptr<LocalTableFunctionState> local_state;
	DataChunk chunk;
	case_insensitive_map_t<idx_t> column_name_to_index;
	idx_t chunk_offset = 0;
};

struct IcebergTablePartitionStatsBindData : public TableFunctionData {
	vector<IcebergTablePartitionStatsFile> files;
	IcebergTableMetadata metadata;
	TableFunction parquet_scan;
	vector<string> file_column_names;
	vector<LogicalType> file_column_types;
};

struct IcebergTablePartitionStatsGlobalState : public GlobalTableFunctionState {
	explicit IcebergTablePartitionStatsGlobalState(const IcebergTablePartitionStatsBindData &bind_data)
	    : bind_data(bind_data) {
	}

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IcebergTablePartitionStatsGlobalState>(
		    input.bind_data->Cast<IcebergTablePartitionStatsBindData>());
	}

	idx_t MaxThreads() const override {
		return 1;
	}

	const IcebergTablePartitionStatsBindData &bind_data;
	idx_t file_index = 0;
};

struct IcebergTablePartitionStatsLocalState : public LocalTableFunctionState {
	explicit IcebergTablePartitionStatsLocalState(ExecutionContext &execution_context)
	    : execution_context(execution_context) {
	}

	static unique_ptr<LocalTableFunctionState> Init(ExecutionContext &context, TableFunctionInitInput &input,
	                                                GlobalTableFunctionState *global_state) {
		return make_uniq<IcebergTablePartitionStatsLocalState>(context);
	}

	ExecutionContext &execution_context;
	unique_ptr<NestedScanState> nested_scan;
};

static constexpr const char *PARTITION_COLUMN = "partition";
static constexpr const char *SPEC_ID_COLUMN = "spec_id";
static constexpr const char *DATA_RECORD_COUNT_COLUMN = "data_record_count";
static constexpr const char *DATA_FILE_COUNT_COLUMN = "data_file_count";
static constexpr const char *TOTAL_DATA_FILE_SIZE_COLUMN = "total_data_file_size_in_bytes";
static constexpr const char *POSITION_DELETE_RECORD_COUNT_COLUMN = "position_delete_record_count";
static constexpr const char *POSITION_DELETE_FILE_COUNT_COLUMN = "position_delete_file_count";
static constexpr const char *DV_COUNT_COLUMN = "dv_count";
static constexpr const char *EQUALITY_DELETE_RECORD_COUNT_COLUMN = "equality_delete_record_count";
static constexpr const char *EQUALITY_DELETE_FILE_COUNT_COLUMN = "equality_delete_file_count";
static constexpr const char *TOTAL_RECORD_COUNT_COLUMN = "total_record_count";
static constexpr const char *LAST_UPDATED_AT_COLUMN = "last_updated_at";
static constexpr const char *LAST_UPDATED_SNAPSHOT_ID_COLUMN = "last_updated_snapshot_id";

static void ParseTableFunctionOptions(IcebergOptions &options, const named_parameter_map_t &named_parameters) {
	auto &snapshot_lookup = options.snapshot_lookup;
	for (auto &kv : named_parameters) {
		auto loption = StringUtil::Lower(kv.first.GetIdentifierName());
		auto &val = kv.second;
		if (loption == "allow_moved_paths") {
			options.allow_moved_paths = BooleanValue::Get(val);
		} else if (loption == "metadata_compression_codec") {
			options.metadata_compression_codec = StringValue::Get(val);
		} else if (loption == "version") {
			options.table_version = StringValue::Get(val);
		} else if (loption == "version_name_format") {
			auto value = StringValue::Get(kv.second);
			auto string_substitutions = IcebergUtils::CountOccurrences(value, "%s");
			if (string_substitutions != 2) {
				throw InvalidInputException(
				    "'version_name_format' has to contain two occurrences of '%%s' in it, found %d",
				    string_substitutions);
			}
			options.version_name_format = value;
		} else if (loption == "snapshot_from_id") {
			if (snapshot_lookup.GetSource() != SnapshotSource::LATEST) {
				throw InvalidInputException(
				    "Can't use 'snapshot_from_id' in combination with 'snapshot_from_timestamp'");
			}
			snapshot_lookup.SetSource(SnapshotSource::FROM_ID);
			snapshot_lookup.snapshot_id = val.GetValue<uint64_t>();
		} else if (loption == "snapshot_from_timestamp") {
			if (snapshot_lookup.GetSource() != SnapshotSource::LATEST) {
				throw InvalidInputException(
				    "Can't use 'snapshot_from_id' in combination with 'snapshot_from_timestamp'");
			}
			snapshot_lookup.SetSource(SnapshotSource::FROM_TIMESTAMP);
			snapshot_lookup.snapshot_timestamp = val.GetValue<timestamp_t>();
		}
	}
}

static string ResolveStatisticsPath(const IcebergTableMetadata &metadata, FileSystem &fs, const IcebergOptions &options,
                                    const string &statistics_path) {
	if (fs.IsPathAbsolute(statistics_path)) {
		return statistics_path;
	}
	if (options.allow_moved_paths) {
		return IcebergUtils::GetFullPath(metadata.GetLocation(), statistics_path, fs);
	}
	return fs.JoinPath(metadata.GetLocation(), statistics_path);
}

static bool SupportsPartitionStatisticsFormat(const string &path) {
	auto lower_path = StringUtil::Lower(path);
	return StringUtil::EndsWith(lower_path, ".parquet");
}

static void PopulateSourceIdToTypeMap(const vector<unique_ptr<IcebergColumnDefinition>> &columns,
                                      unordered_map<uint64_t, const LogicalType *> &source_id_to_type) {
	for (auto &col : columns) {
		source_id_to_type.emplace(static_cast<uint64_t>(col->id), &col->type);
		PopulateSourceIdToTypeMap(col->GetChildren(), source_id_to_type);
	}
}

static LogicalType BuildUnifiedPartitionType(const IcebergTableMetadata &metadata) {
	unordered_map<uint64_t, const LogicalType *> source_id_to_type;
	for (auto &schema_pair : metadata.GetSchemas()) {
		PopulateSourceIdToTypeMap(schema_pair.second->columns, source_id_to_type);
	}

	map<uint64_t, pair<string, LogicalType>> fields_by_id;
	for (auto &spec_pair : metadata.GetPartitionSpecs()) {
		for (auto &field : spec_pair.second.GetFields()) {
			auto type_it = source_id_to_type.find(field.source_id);
			if (type_it == source_id_to_type.end()) {
				throw InvalidConfigurationException(
				    "Partition statistics field '%s' references source column id %llu, but that column was not found "
				    "in any table schema",
				    field.GetPartitionSpecFieldName(), field.source_id);
			}
			auto result_type = field.transform.GetSerializedType(*type_it->second);
			fields_by_id.emplace(field.partition_field_id, make_pair(field.GetPartitionSpecFieldName(), result_type));
		}
	}

	child_list_t<LogicalType> children;
	for (auto &entry : fields_by_id) {
		children.emplace_back(entry.second.first, entry.second.second);
	}
	return LogicalType::STRUCT(std::move(children));
}

static TableFunction GetParquetScanFunction(ClientContext &context) {
	auto &instance = DatabaseInstance::GetDatabase(context);
	auto &system_catalog = Catalog::GetSystemCatalog(instance);
	auto data = CatalogTransaction::GetSystemTransaction(instance);
	auto &schema = system_catalog.GetSchema(data, Identifier::DefaultSchema());
	auto catalog_entry = schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, "parquet_scan");
	if (!catalog_entry) {
		throw InvalidInputException("Function with name \"parquet_scan\" not found!");
	}
	return catalog_entry->Cast<TableFunctionCatalogEntry>().functions.functions[0];
}

static unique_ptr<NestedScanState> InitializeNestedScan(const IcebergTablePartitionStatsBindData &bind_data,
                                                        IcebergTablePartitionStatsLocalState &local_state,
                                                        const IcebergTablePartitionStatsFile &file) {
	if (!SupportsPartitionStatisticsFormat(file.statistics_path)) {
		throw NotImplementedException(
		    "Only Parquet-backed Iceberg partition statistics files are currently supported, got '%s'",
		    file.statistics_path);
	}

	vector<Value> children;
	children.emplace_back(file.statistics_path);

	named_parameter_map_t named_params;
	vector<LogicalType> input_types;
	vector<Identifier> input_names;
	TableFunctionRef empty;
	auto parquet_scan = bind_data.parquet_scan;
	vector<LogicalType> return_types;
	vector<string> return_names;
	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr, parquet_scan,
	                                  empty);
	auto nested = make_uniq<NestedScanState>();
	nested->function = parquet_scan;
	nested->bind_data = parquet_scan.bind(local_state.execution_context.client, bind_input, return_types, return_names);

	vector<column_t> column_ids;
	column_ids.reserve(return_types.size());
	for (idx_t i = 0; i < return_types.size(); i++) {
		column_ids.push_back(i);
		nested->column_name_to_index.emplace(return_names[i], i);
	}

	TableFunctionInitInput init_input(nested->bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	nested->global_state = parquet_scan.init_global(local_state.execution_context.client, init_input);
	nested->local_state =
	    parquet_scan.init_local(local_state.execution_context, init_input, nested->global_state.get());
	nested->chunk.Initialize(local_state.execution_context.client, return_types, STANDARD_VECTOR_SIZE);
	return nested;
}

static void FillMetadataColumn(Vector &vector, idx_t offset, idx_t count, const Value &value) {
	for (idx_t i = 0; i < count; i++) {
		vector.SetValue(offset + i, value);
	}
}

static void FillNullColumn(Vector &vector, idx_t offset, idx_t count, const LogicalType &type) {
	auto null_value = Value(type);
	for (idx_t i = 0; i < count; i++) {
		vector.SetValue(offset + i, null_value);
	}
}

static void CopyColumnValues(const Vector &source, Vector &target, idx_t source_offset, idx_t target_offset,
                             idx_t count) {
	for (idx_t i = 0; i < count; i++) {
		target.SetValue(target_offset + i, source.GetValue(source_offset + i));
	}
}

static unique_ptr<FunctionData> IcebergTablePartitionStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                               vector<LogicalType> &return_types,
                                                               vector<string> &names) {
	auto bind_data = make_uniq<IcebergTablePartitionStatsBindData>();

	auto &fs = FileSystem::GetFileSystem(context);
	auto caching_fs = make_shared_ptr<CachingFileSystemWrapper>(fs, *context.db);
	auto input_string = input.inputs[0].ToString();
	auto filename = IcebergUtils::GetStorageLocation(context, input_string);

	IcebergOptions options;
	ParseTableFunctionOptions(options, input.named_parameters);

	auto iceberg_meta_path = IcebergTableMetadata::GetMetaDataPath(context, filename, fs, options);
	auto table_metadata =
	    IcebergTableMetadata::Parse(iceberg_meta_path, *caching_fs, options.metadata_compression_codec);
	bind_data->metadata = IcebergTableMetadata::FromTableMetadata(table_metadata);
	bind_data->parquet_scan = GetParquetScanFunction(context);

	optional<int64_t> requested_snapshot_id;
	bool no_matching_snapshot = false;
	if (!options.snapshot_lookup.IsLatest()) {
		auto snapshot = bind_data->metadata.GetSnapshot(options.snapshot_lookup);
		if (!snapshot.snapshot) {
			no_matching_snapshot = true;
		} else {
			requested_snapshot_id = snapshot.snapshot->snapshot_id;
		}
	}

	if (!no_matching_snapshot) {
		for (auto &statistics_file : bind_data->metadata.partition_statistics) {
			if (requested_snapshot_id && statistics_file.snapshot_id != *requested_snapshot_id) {
				continue;
			}
			IcebergTablePartitionStatsFile file;
			file.snapshot_id = statistics_file.snapshot_id;
			file.statistics_path =
			    ResolveStatisticsPath(bind_data->metadata, fs, options, statistics_file.statistics_path);
			file.file_size_in_bytes = statistics_file.file_size_in_bytes;
			bind_data->files.emplace_back(std::move(file));
		}
	}

	names.emplace_back("statistics_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("statistics_path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("file_size_in_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	auto partition_type = BuildUnifiedPartitionType(bind_data->metadata);
	names.emplace_back(PARTITION_COLUMN);
	return_types.emplace_back(std::move(partition_type));
	names.emplace_back(SPEC_ID_COLUMN);
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back(DATA_RECORD_COUNT_COLUMN);
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back(DATA_FILE_COUNT_COLUMN);
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back(TOTAL_DATA_FILE_SIZE_COLUMN);
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back(POSITION_DELETE_RECORD_COUNT_COLUMN);
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back(POSITION_DELETE_FILE_COUNT_COLUMN);
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back(DV_COUNT_COLUMN);
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back(EQUALITY_DELETE_RECORD_COUNT_COLUMN);
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back(EQUALITY_DELETE_FILE_COUNT_COLUMN);
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back(TOTAL_RECORD_COUNT_COLUMN);
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back(LAST_UPDATED_AT_COLUMN);
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back(LAST_UPDATED_SNAPSHOT_ID_COLUMN);
	return_types.emplace_back(LogicalType::BIGINT);

	bind_data->file_column_names = {PARTITION_COLUMN,
	                                SPEC_ID_COLUMN,
	                                DATA_RECORD_COUNT_COLUMN,
	                                DATA_FILE_COUNT_COLUMN,
	                                TOTAL_DATA_FILE_SIZE_COLUMN,
	                                POSITION_DELETE_RECORD_COUNT_COLUMN,
	                                POSITION_DELETE_FILE_COUNT_COLUMN,
	                                DV_COUNT_COLUMN,
	                                EQUALITY_DELETE_RECORD_COUNT_COLUMN,
	                                EQUALITY_DELETE_FILE_COUNT_COLUMN,
	                                TOTAL_RECORD_COUNT_COLUMN,
	                                LAST_UPDATED_AT_COLUMN,
	                                LAST_UPDATED_SNAPSHOT_ID_COLUMN};
	for (idx_t i = 3; i < return_types.size(); i++) {
		bind_data->file_column_types.push_back(return_types[i]);
	}

	return std::move(bind_data);
}

static void ValidateRequiredColumn(const NestedScanState &nested_scan, const string &path, const char *column_name) {
	if (!nested_scan.column_name_to_index.count(column_name)) {
		throw InvalidConfigurationException("Partition statistics file '%s' is missing required column '%s'", path,
		                                    column_name);
	}
}

static void EnsureRequiredColumnsPresent(const NestedScanState &nested_scan, const string &path) {
	ValidateRequiredColumn(nested_scan, path, PARTITION_COLUMN);
	ValidateRequiredColumn(nested_scan, path, SPEC_ID_COLUMN);
	ValidateRequiredColumn(nested_scan, path, DATA_RECORD_COUNT_COLUMN);
	ValidateRequiredColumn(nested_scan, path, DATA_FILE_COUNT_COLUMN);
	ValidateRequiredColumn(nested_scan, path, TOTAL_DATA_FILE_SIZE_COLUMN);
}

static void IcebergTablePartitionStatsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergTablePartitionStatsBindData>();
	auto &global_state = data.global_state->Cast<IcebergTablePartitionStatsGlobalState>();
	auto &local_state = data.local_state->Cast<IcebergTablePartitionStatsLocalState>();
	auto &fs = FileSystem::GetFileSystem(context);

	idx_t out = 0;
	while (out < STANDARD_VECTOR_SIZE) {
		if (!local_state.nested_scan) {
			if (global_state.file_index >= bind_data.files.size()) {
				break;
			}
			auto &file = bind_data.files[global_state.file_index];
			auto file_handle = fs.OpenFile(file.statistics_path, FileFlags::FILE_FLAGS_READ);
			auto actual_size = NumericCast<int64_t>(fs.GetFileSize(*file_handle));
			if (actual_size != file.file_size_in_bytes) {
				throw InvalidConfigurationException(
				    "Partition statistics file '%s' has size %lld bytes, but the table metadata registered %lld bytes",
				    file.statistics_path, actual_size, file.file_size_in_bytes);
			}
			local_state.nested_scan = InitializeNestedScan(bind_data, local_state, file);
			EnsureRequiredColumnsPresent(*local_state.nested_scan, file.statistics_path);
		}

		auto &nested_scan = *local_state.nested_scan;
		if (nested_scan.chunk_offset >= nested_scan.chunk.size()) {
			nested_scan.chunk.Reset();
			TableFunctionInput nested_input(nested_scan.bind_data.get(), nested_scan.local_state.get(),
			                                nested_scan.global_state.get());
			nested_scan.function.function(context, nested_input, nested_scan.chunk);
			nested_scan.chunk.Flatten();
			nested_scan.chunk_offset = 0;
			if (nested_scan.chunk.size() == 0) {
				local_state.nested_scan.reset();
				global_state.file_index++;
				continue;
			}
		}

		auto &file = bind_data.files[global_state.file_index];
		auto copy_count =
		    MinValue<idx_t>(STANDARD_VECTOR_SIZE - out, nested_scan.chunk.size() - nested_scan.chunk_offset);
		FillMetadataColumn(output.data[0], out, copy_count, Value::BIGINT(file.snapshot_id));
		FillMetadataColumn(output.data[1], out, copy_count, Value(file.statistics_path));
		FillMetadataColumn(output.data[2], out, copy_count, Value::BIGINT(file.file_size_in_bytes));

		for (idx_t i = 0; i < bind_data.file_column_names.size(); i++) {
			auto target_col = i + 3;
			auto source_it = nested_scan.column_name_to_index.find(bind_data.file_column_names[i]);
			if (source_it == nested_scan.column_name_to_index.end()) {
				FillNullColumn(output.data[target_col], out, copy_count, bind_data.file_column_types[i]);
				continue;
			}
			CopyColumnValues(nested_scan.chunk.data[source_it->second], output.data[target_col],
			                 nested_scan.chunk_offset, out, copy_count);
		}

		out += copy_count;
		nested_scan.chunk_offset += copy_count;
	}
	output.SetChildCardinality(out);
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergTablePartitionStatsFunction() {
	TableFunctionSet function_set("iceberg_table_partition_stats");
	TableFunction fun({LogicalType::VARCHAR}, IcebergTablePartitionStatsFunction, IcebergTablePartitionStatsBind,
	                  IcebergTablePartitionStatsGlobalState::Init, IcebergTablePartitionStatsLocalState::Init);

	fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	fun.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	fun.named_parameters["version"] = LogicalType::VARCHAR;
	fun.named_parameters["version_name_format"] = LogicalType::VARCHAR;
	fun.named_parameters["snapshot_from_timestamp"] = LogicalType::TIMESTAMP;
	fun.named_parameters["snapshot_from_id"] = LogicalType::UBIGINT;
	function_set.AddFunction(fun);
	return function_set;
}

} // namespace duckdb
