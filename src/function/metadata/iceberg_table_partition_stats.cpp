#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system_wrapper.hpp"

#include "function/iceberg_functions.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "iceberg_options.hpp"
#include "function/metadata/iceberg_table_partition_stats/iceberg_partition_stats_multi_file_list.hpp"
#include "function/metadata/iceberg_table_partition_stats/iceberg_partition_stats_multi_file_reader.hpp"

namespace duckdb {

namespace {

static constexpr const char *SCAN_FILENAME_COLUMN = "__partition_stats_filename";
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

struct PartitionStatsFileMetadata {
	int64_t snapshot_id;
	string statistics_path;
	int64_t file_size_in_bytes;
};

struct IcebergTablePartitionStatsBindData : public TableFunctionData {
	TableFunction parquet_scan;
	vector<string> paths;
	unordered_map<string, PartitionStatsFileMetadata> file_metadata;
	shared_ptr<IcebergPartitionStatsScanInfo> scan_info;
	case_insensitive_map_t<idx_t> nested_column_name_to_index;
	vector<string> output_column_names;
	vector<LogicalType> output_column_types;
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
};

struct IcebergTablePartitionStatsLocalState : public LocalTableFunctionState {
	static unique_ptr<LocalTableFunctionState> Init(ExecutionContext &context, TableFunctionInitInput &input,
	                                                GlobalTableFunctionState *global_state) {
		auto &bind_data = input.bind_data->Cast<IcebergTablePartitionStatsBindData>();
		auto result = make_uniq<IcebergTablePartitionStatsLocalState>();
		result->InitializeNestedScan(context, bind_data);
		return std::move(result);
	}

	void InitializeNestedScan(ExecutionContext &context, const IcebergTablePartitionStatsBindData &bind_data);

	unique_ptr<FunctionData> nested_bind_data;
	unique_ptr<GlobalTableFunctionState> nested_global_state;
	unique_ptr<LocalTableFunctionState> nested_local_state;
	DataChunk nested_chunk;
};

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

static vector<Value> BuildPathListValues(const vector<string> &paths) {
	vector<Value> result;
	result.reserve(paths.size());
	for (auto &path : paths) {
		result.emplace_back(path);
	}
	return result;
}

static unique_ptr<FunctionData> BindParquetScan(ClientContext &context,
                                                const IcebergTablePartitionStatsBindData &bind_data,
                                                vector<LogicalType> &return_types, vector<string> &return_names) {
	vector<Value> children;
	children.emplace_back(Value::LIST(LogicalType::VARCHAR, BuildPathListValues(bind_data.paths)));

	named_parameter_map_t named_params;
	named_params["filename"] = Value(SCAN_FILENAME_COLUMN);

	vector<LogicalType> input_types;
	vector<Identifier> input_names;
	TableFunctionRef empty;
	TableFunction dummy_table_function;
	dummy_table_function.name = Identifier("IcebergPartitionStatsParquet");
	dummy_table_function.get_multi_file_reader = IcebergPartitionStatsMultiFileReader::CreateInstance;
	dummy_table_function.function_info = bind_data.scan_info;

	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  dummy_table_function, empty);
	return bind_data.parquet_scan.bind(context, bind_input, return_types, return_names);
}

void IcebergTablePartitionStatsLocalState::InitializeNestedScan(ExecutionContext &context,
                                                                const IcebergTablePartitionStatsBindData &bind_data) {
	vector<LogicalType> return_types;
	vector<string> return_names;
	if (bind_data.paths.empty()) {
		return;
	}
	nested_bind_data = BindParquetScan(context.client, bind_data, return_types, return_names);

	vector<column_t> column_ids;
	column_ids.reserve(return_types.size());
	for (idx_t i = 0; i < return_types.size(); i++) {
		column_ids.push_back(i);
	}

	TableFunctionInitInput init_input(nested_bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	nested_global_state = bind_data.parquet_scan.init_global(context.client, init_input);
	nested_local_state = bind_data.parquet_scan.init_local(context, init_input, nested_global_state.get());
	nested_chunk.Initialize(context.client, return_types, STANDARD_VECTOR_SIZE);
}

static void ValidateRequiredColumn(const case_insensitive_map_t<idx_t> &column_map, const char *column_name) {
	if (!column_map.count(column_name)) {
		throw InvalidConfigurationException("Partition statistics scan is missing required column '%s'", column_name);
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

	IcebergOptions options(input.named_parameters);

	auto iceberg_meta_path = IcebergTableMetadata::GetMetaDataPath(context, filename, fs, options);
	auto table_metadata =
	    IcebergTableMetadata::Parse(iceberg_meta_path, *caching_fs, options.metadata_compression_codec);
	auto metadata = IcebergTableMetadata::FromTableMetadata(table_metadata);
	bind_data->parquet_scan = GetParquetScanFunction(context);

	optional<int64_t> requested_snapshot_id;
	bool no_matching_snapshot = false;
	if (!options.snapshot_lookup->IsLatest()) {
		auto snapshot = metadata.GetSnapshot(*options.snapshot_lookup);
		if (!snapshot.snapshot) {
			no_matching_snapshot = true;
		} else {
			requested_snapshot_id = snapshot.snapshot->snapshot_id;
		}
	}

	if (!no_matching_snapshot) {
		vector<IcebergPartitionStatsScanFile> scan_files;
		for (auto &statistics_file : metadata.partition_statistics) {
			if (requested_snapshot_id && statistics_file.snapshot_id != *requested_snapshot_id) {
				continue;
			}
			auto resolved_path = ResolveStatisticsPath(metadata, fs, options, statistics_file.statistics_path);
			if (!SupportsPartitionStatisticsFormat(resolved_path)) {
				throw NotImplementedException(
				    "Only Parquet-backed Iceberg partition statistics files are currently supported, got '%s'",
				    resolved_path);
			}

			auto file_handle = fs.OpenFile(resolved_path, FileFlags::FILE_FLAGS_READ);
			auto actual_size = NumericCast<int64_t>(fs.GetFileSize(*file_handle));
			if (actual_size != statistics_file.file_size_in_bytes) {
				throw InvalidConfigurationException(
				    "Partition statistics file '%s' has size %lld bytes, but the table metadata registered %lld bytes",
				    resolved_path, actual_size, statistics_file.file_size_in_bytes);
			}

			PartitionStatsFileMetadata file_metadata;
			file_metadata.snapshot_id = statistics_file.snapshot_id;
			file_metadata.statistics_path = resolved_path;
			file_metadata.file_size_in_bytes = statistics_file.file_size_in_bytes;
			bind_data->paths.push_back(resolved_path);
			bind_data->file_metadata.emplace(resolved_path, std::move(file_metadata));
			scan_files.push_back({resolved_path, statistics_file.file_size_in_bytes});
		}
		bind_data->scan_info = make_shared_ptr<IcebergPartitionStatsScanInfo>(std::move(scan_files));
	}

	if (!bind_data->paths.empty()) {
		vector<LogicalType> nested_return_types;
		vector<string> nested_return_names;
		auto nested_bind_data = BindParquetScan(context, *bind_data, nested_return_types, nested_return_names);
		(void)nested_bind_data;

		for (idx_t i = 0; i < nested_return_names.size(); i++) {
			bind_data->nested_column_name_to_index.emplace(nested_return_names[i], i);
		}

		ValidateRequiredColumn(bind_data->nested_column_name_to_index, PARTITION_COLUMN);
		ValidateRequiredColumn(bind_data->nested_column_name_to_index, SPEC_ID_COLUMN);
		ValidateRequiredColumn(bind_data->nested_column_name_to_index, DATA_RECORD_COUNT_COLUMN);
		ValidateRequiredColumn(bind_data->nested_column_name_to_index, DATA_FILE_COUNT_COLUMN);
		ValidateRequiredColumn(bind_data->nested_column_name_to_index, TOTAL_DATA_FILE_SIZE_COLUMN);
		ValidateRequiredColumn(bind_data->nested_column_name_to_index, SCAN_FILENAME_COLUMN);
	}

	names = {"statistics_snapshot_id",
	         "statistics_path",
	         "file_size_in_bytes",
	         PARTITION_COLUMN,
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

	return_types = {LogicalType::BIGINT,  LogicalType::VARCHAR, LogicalType::BIGINT,  LogicalType::VARCHAR,
	                LogicalType::INTEGER, LogicalType::BIGINT,  LogicalType::INTEGER, LogicalType::BIGINT,
	                LogicalType::BIGINT,  LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::BIGINT,
	                LogicalType::INTEGER, LogicalType::BIGINT,  LogicalType::BIGINT,  LogicalType::BIGINT};

	bind_data->output_column_names = names;
	bind_data->output_column_types = return_types;
	return std::move(bind_data);
}

static void SetMetadataValue(Vector &vector, idx_t row_idx, const PartitionStatsFileMetadata &metadata,
                             idx_t column_idx) {
	switch (column_idx) {
	case 0:
		vector.SetValue(row_idx, Value::BIGINT(metadata.snapshot_id));
		return;
	case 1:
		vector.SetValue(row_idx, Value(metadata.statistics_path));
		return;
	case 2:
		vector.SetValue(row_idx, Value::BIGINT(metadata.file_size_in_bytes));
		return;
	default:
		throw InternalException("Unexpected metadata column index");
	}
}

static void IcebergTablePartitionStatsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergTablePartitionStatsBindData>();
	auto &local_state = data.local_state->Cast<IcebergTablePartitionStatsLocalState>();
	if (bind_data.paths.empty()) {
		return;
	}

	local_state.nested_chunk.Reset();
	TableFunctionInput nested_input(local_state.nested_bind_data.get(), local_state.nested_local_state.get(),
	                                local_state.nested_global_state.get());
	bind_data.parquet_scan.function(context, nested_input, local_state.nested_chunk);
	local_state.nested_chunk.Flatten();
	if (local_state.nested_chunk.size() == 0) {
		return;
	}

	for (idx_t row_idx = 0; row_idx < local_state.nested_chunk.size(); row_idx++) {
		auto path = local_state.nested_chunk.data[bind_data.nested_column_name_to_index.at(SCAN_FILENAME_COLUMN)]
		                .GetValue(row_idx)
		                .ToString();
		auto metadata_it = bind_data.file_metadata.find(path);
		if (metadata_it == bind_data.file_metadata.end()) {
			throw InvalidConfigurationException(
			    "Scanned partition statistics file '%s' was not registered in table metadata", path);
		}

		for (idx_t col_idx = 0; col_idx < bind_data.output_column_names.size(); col_idx++) {
			if (col_idx < 3) {
				SetMetadataValue(output.data[col_idx], row_idx, metadata_it->second, col_idx);
				continue;
			}
			auto nested_name = bind_data.output_column_names[col_idx];
			auto nested_it = bind_data.nested_column_name_to_index.find(nested_name);
			if (nested_it == bind_data.nested_column_name_to_index.end()) {
				output.data[col_idx].SetValue(row_idx, Value(bind_data.output_column_types[col_idx]));
			} else {
				output.data[col_idx].SetValue(row_idx,
				                              local_state.nested_chunk.data[nested_it->second].GetValue(row_idx));
			}
		}
	}
	output.SetChildCardinality(local_state.nested_chunk.size());
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
