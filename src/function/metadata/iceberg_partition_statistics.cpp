#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/partition/iceberg_partition_struct.hpp"
#include "function/iceberg_functions.hpp"
#include "iceberg_options.hpp"
#include "planning/iceberg_file_secret.hpp"

namespace duckdb {

namespace {

//! Opens the one partition statistics file of a scan with the table's file secret
struct IcebergPartitionStatisticsScanInfo : public TableFunctionInfo {
	OpenFileInfo file;
};

class IcebergPartitionStatisticsFileReader : public MultiFileReader {
public:
	explicit IcebergPartitionStatisticsFileReader(shared_ptr<TableFunctionInfo> function_info)
	    : function_info(std::move(function_info)) {
	}

	static unique_ptr<MultiFileReader> CreateInstance(const BoundTableFunction &table) {
		return make_uniq<IcebergPartitionStatisticsFileReader>(table.function_info);
	}

	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &glob_input) override {
		auto &scan_info = function_info->Cast<IcebergPartitionStatisticsScanInfo>();
		vector<OpenFileInfo> files;
		files.push_back(scan_info.file);
		return make_shared_ptr<SimpleMultiFileList>(std::move(files));
	}

	void BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	                 vector<Identifier> &names, MultiFileReaderBindData &bind_data) override {
		options.auto_detect_hive_partitioning = false;
		options.hive_partitioning = false;
		options.union_by_name = false;
		MultiFileReader::BindOptions(options, files, return_types, names, bind_data);
	}

private:
	shared_ptr<TableFunctionInfo> function_info;
};

//! A partition statistics file with the path it is read from
struct IcebergPartitionStatisticsEntry {
	int64_t snapshot_id;
	string path;
	int64_t file_size_in_bytes;
};

struct IcebergPartitionStatisticsBindData : public TableFunctionData {
	vector<IcebergPartitionStatisticsEntry> files;
	IcebergFileSecret file_secret;
	shared_ptr<const IcebergTableMetadata> metadata;
	vector<IcebergPartitionStructField> partition_fields;
	LogicalType partition_type;
	vector<string> names;
	vector<LogicalType> types;
};

//! The nested Parquet scan of one statistics file
struct IcebergPartitionStatisticsFileScan {
	TableFunction function;
	unique_ptr<FunctionData> bind_data;
	unique_ptr<GlobalTableFunctionState> global_state;
	unique_ptr<LocalTableFunctionState> local_state;
	//! The file's column names, by position in the chunk
	vector<string> names;
	DataChunk chunk;
};

struct IcebergPartitionStatisticsGlobalState : public GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IcebergPartitionStatisticsGlobalState>();
	}

	idx_t MaxThreads() const override {
		return 1;
	}

	//! Index of the next file to open
	idx_t file_index = 0;
	unique_ptr<IcebergPartitionStatisticsFileScan> current;
	//! Row of the current chunk that is returned next
	idx_t chunk_offset = 0;
};

//! A path without a scheme and not starting at the root is relative to the table location; a full path is moved
//! under the table location only with allow_moved_paths
static string ResolveStatisticsPath(const string &table_location, const IcebergOptions &options, FileSystem &fs,
                                    const string &path) {
	const bool full_path =
	    FileSystem::IsRemoteFile(path) || StringUtil::StartsWith(path, "/") || fs.IsPathAbsolute(path);
	if (!full_path) {
		return fs.JoinPath(table_location, path);
	}
	if (options.allow_moved_paths) {
		return IcebergUtils::GetFullPath(table_location, path, fs);
	}
	return path;
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
	auto &parquet_scan = catalog_entry->Cast<TableFunctionCatalogEntry>();
	auto function = *parquet_scan.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});
	function.get_multi_file_reader = IcebergPartitionStatisticsFileReader::CreateInstance;
	function.statistics = nullptr;
	function.statistics_extended = nullptr;
	function.table_scan_progress = nullptr;
	function.get_bind_info = nullptr;
	return function;
}

static unique_ptr<IcebergPartitionStatisticsFileScan> OpenFileScan(ExecutionContext &execution_context,
                                                                   const IcebergPartitionStatisticsBindData &bind_data,
                                                                   const IcebergPartitionStatisticsEntry &entry) {
	auto &context = execution_context.client;
	auto scan = make_uniq<IcebergPartitionStatisticsFileScan>();
	scan->function = GetParquetScanFunction(context);

	auto scan_info = make_shared_ptr<IcebergPartitionStatisticsScanInfo>();
	scan_info->file = OpenFileInfo(entry.path);
	scan_info->file.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	bind_data.file_secret.Apply(scan_info->file);
	scan_info->file.extended_info->options["file_size"] = Value::UBIGINT(NumericCast<idx_t>(entry.file_size_in_bytes));
	scan_info->file.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
	scan->function.function_info = scan_info;

	vector<Value> children;
	children.emplace_back(entry.path);
	named_argument_map_t named_params;
	vector<LogicalType> input_types;
	vector<Identifier> input_names;
	TableFunctionRef empty;
	BoundTableFunction bound_function(scan->function);
	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  bound_function, empty);
	vector<LogicalType> return_types;
	vector<Identifier> return_names;
	scan->bind_data = scan->function.bind(context, bind_input, return_types, return_names);
	scan->names = IdentifiersToStrings(return_names);

	vector<column_t> column_ids;
	for (idx_t i = 0; i < return_types.size(); i++) {
		column_ids.push_back(i);
	}
	TableFunctionInitInput init_input(scan->bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	scan->global_state = scan->function.init_global(context, init_input);
	scan->local_state = scan->function.init_local(execution_context, init_input, scan->global_state.get());
	scan->chunk.Initialize(context, return_types, STANDARD_VECTOR_SIZE);
	return scan;
}

static optional_idx FindColumn(const vector<string> &names, const string &name) {
	for (idx_t i = 0; i < names.size(); i++) {
		if (StringUtil::CIEquals(names[i], name)) {
			return i;
		}
	}
	return optional_idx();
}

//! The file's partition struct as the table's unified partition struct: fields are matched by name, a field the
//! file does not have (added by a later partition spec) is NULL
static Value UnifiedPartitionValue(const Value &file_partition, const vector<IcebergPartitionStructField> &fields) {
	child_list_t<Value> children;
	auto &file_type = file_partition.type();
	for (auto &field : fields) {
		Value value(field.type);
		if (!file_partition.IsNull() && file_type.id() == LogicalTypeId::STRUCT) {
			auto &file_children = StructValue::GetChildren(file_partition);
			for (idx_t i = 0; i < file_children.size(); i++) {
				if (StructType::GetChildName(file_type, i) != field.name || file_children[i].IsNull()) {
					continue;
				}
				value = file_children[i].DefaultCastAs(field.type);
				break;
			}
		}
		children.emplace_back(field.name, std::move(value));
	}
	return Value::STRUCT(std::move(children));
}

static unique_ptr<FunctionData> IcebergPartitionStatisticsBind(ClientContext &context, TableFunctionBindInput &input,
                                                               vector<LogicalType> &return_types,
                                                               vector<Identifier> &names) {
	auto bind_data = make_uniq<IcebergPartitionStatisticsBindData>();
	auto input_string = input.inputs[0].ToString();
	IcebergOptions options(input.named_parameters);
	auto resolved = IcebergUtils::ResolveTableMetadata(context, input_string, options);
	bind_data->metadata = resolved.metadata;
	bind_data->file_secret = resolved.file_secret;
	auto &metadata = *bind_data->metadata;
	auto &fs = FileSystem::GetFileSystem(context);

	//! Every partition statistics file of the table; a snapshot lookup narrows it to the file of that snapshot
	optional<int64_t> snapshot_id;
	bool snapshot_found = true;
	if (!options.snapshot_lookup->IsLatest()) {
		auto snapshot = metadata.GetSnapshot(*options.snapshot_lookup);
		snapshot_found = snapshot.snapshot != nullptr;
		if (snapshot_found) {
			snapshot_id = snapshot.snapshot->snapshot_id;
		}
	}
	if (snapshot_found) {
		for (auto &file : metadata.partition_statistics) {
			if (snapshot_id && file.snapshot_id != *snapshot_id) {
				continue;
			}
			auto path = ResolveStatisticsPath(resolved.table_location, options, fs, file.path);
			if (!StringUtil::EndsWith(StringUtil::Lower(path), ".parquet")) {
				throw NotImplementedException("Partition statistics file '%s' is not a Parquet file", path);
			}
			//! The file must be the one the table metadata registered
			auto open_file = OpenFileInfo(path);
			resolved.file_secret.Apply(open_file);
			auto handle = fs.OpenFile(open_file, FileFlags::FILE_FLAGS_READ);
			auto actual_size = NumericCast<int64_t>(fs.GetFileSize(*handle));
			if (actual_size != file.file_size_in_bytes) {
				throw InvalidConfigurationException("Partition statistics file '%s' has %lld bytes, but the table "
				                                    "metadata registers %lld bytes",
				                                    path, actual_size, file.file_size_in_bytes);
			}
			bind_data->files.push_back({file.snapshot_id, path, file.file_size_in_bytes});
		}
	}

	bind_data->partition_fields = IcebergPartitionStruct::GetFields(metadata);
	names.emplace_back("statistics_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("statistics_path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("file_size_in_bytes");
	return_types.emplace_back(LogicalType::BIGINT);
	//! Unpartitioned tables have no 'partition', as in iceberg_partitions
	if (!bind_data->partition_fields.empty()) {
		bind_data->partition_type = IcebergPartitionStruct::GetType(bind_data->partition_fields);
		names.emplace_back("partition");
		return_types.emplace_back(bind_data->partition_type);
	}
	names.emplace_back("spec_id");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("data_record_count");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("data_file_count");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("total_data_file_size_in_bytes");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("position_delete_record_count");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("position_delete_file_count");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("equality_delete_record_count");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("equality_delete_file_count");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("total_record_count");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("last_updated_at");
	return_types.emplace_back(LogicalType::TIMESTAMP_MS);
	names.emplace_back("last_updated_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("dv_count");
	return_types.emplace_back(LogicalType::INTEGER);
	bind_data->names = IdentifiersToStrings(names);
	bind_data->types = return_types;
	return std::move(bind_data);
}

static void IcebergPartitionStatisticsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergPartitionStatisticsBindData>();
	auto &state = data.global_state->Cast<IcebergPartitionStatisticsGlobalState>();
	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);

	idx_t out = 0;
	while (out < STANDARD_VECTOR_SIZE) {
		if (!state.current) {
			if (state.file_index >= bind_data.files.size()) {
				break;
			}
			state.current = OpenFileScan(execution_context, bind_data, bind_data.files[state.file_index]);
			state.chunk_offset = 0;
		}
		auto &scan = *state.current;
		if (state.chunk_offset >= scan.chunk.size()) {
			scan.chunk.Reset();
			TableFunctionInput scan_input(scan.bind_data.get(), scan.local_state.get(), scan.global_state.get());
			scan.function.function(context, scan_input, scan.chunk);
			scan.chunk.Flatten();
			state.chunk_offset = 0;
			if (scan.chunk.size() == 0) {
				state.current.reset();
				state.file_index++;
				continue;
			}
		}

		auto &file = bind_data.files[state.file_index];
		auto row = state.chunk_offset++;
		idx_t col = 0;
		output.data[col++].SetValue(out, Value::BIGINT(file.snapshot_id));
		output.data[col++].SetValue(out, Value(file.path));
		output.data[col++].SetValue(out, Value::BIGINT(file.file_size_in_bytes));
		if (!bind_data.partition_fields.empty()) {
			auto partition_column = FindColumn(scan.names, "partition");
			Value partition(LogicalType::SQLNULL);
			if (partition_column.IsValid()) {
				partition = scan.chunk.data[partition_column.GetIndex()].GetValue(row);
			}
			output.data[col++].SetValue(out, UnifiedPartitionValue(partition, bind_data.partition_fields));
		}
		//! The remaining output columns carry the file's column of the same name; a column the file lacks is NULL
		for (; col < bind_data.names.size(); col++) {
			auto &type = bind_data.types[col];
			auto file_column = FindColumn(scan.names, bind_data.names[col]);
			if (!file_column.IsValid()) {
				output.data[col].SetValue(out, Value(type));
				continue;
			}
			auto value = scan.chunk.data[file_column.GetIndex()].GetValue(row);
			if (value.IsNull()) {
				output.data[col].SetValue(out, Value(type));
			} else if (type.id() == LogicalTypeId::TIMESTAMP_MS && value.type().IsIntegral()) {
				//! The spec stores last_updated_at as milliseconds since the epoch
				output.data[col].SetValue(out, Value::TIMESTAMPMS(timestamp_ms_t(value.GetValue<int64_t>())));
			} else {
				output.data[col].SetValue(out, value.DefaultCastAs(type));
			}
		}
		out++;
	}
	output.SetChildCardinality(out);
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergPartitionStatisticsFunction() {
	TableFunctionSet function_set("iceberg_partition_statistics");
	TableFunction table_function(FunctionSignature().AddPositionalOnly("path", LogicalType::VARCHAR),
	                             IcebergPartitionStatisticsFunction, IcebergPartitionStatisticsBind,
	                             IcebergPartitionStatisticsGlobalState::Init);
	table_function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("allow_moved_paths", LogicalType::BOOLEAN)
		    .Add("metadata_compression_codec", LogicalType::VARCHAR)
		    .Add("snapshot_from_id", LogicalType::UBIGINT)
		    .Add("snapshot_from_timestamp", LogicalType::TIMESTAMP)
		    .Add("version", LogicalType::ANY)
		    .Add("version_name_format", LogicalType::VARCHAR);
	});
	function_set.AddFunction(table_function);
	return function_set;
}

} // namespace duckdb
