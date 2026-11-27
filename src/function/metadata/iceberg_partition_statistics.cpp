#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
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
	//! Iceberg IDs and fallback names of the columns read from Parquet, in output order
	vector<pair<int32_t, string>> file_columns;
};

//! The nested Parquet scan of one statistics file
struct IcebergPartitionStatisticsFileScan {
	TableFunction function;
	unique_ptr<FunctionData> bind_data;
	unique_ptr<GlobalTableFunctionState> global_state;
	unique_ptr<LocalTableFunctionState> local_state;
	vector<optional_idx> column_indexes;
	vector<optional_idx> partition_indexes;
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

//! Parquet field IDs survive renames. Older writers may omit IDs, in which case use the column's name.
static optional_idx FindColumn(const vector<MultiFileColumnDefinition> &columns, int64_t id, const string &name) {
	optional_idx by_name;
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &column = columns[i];
		if (!column.identifier.IsNull()) {
			if (column.GetIdentifierFieldId() == id) {
				return i;
			}
		} else if (StringUtil::CIEquals(column.name.GetIdentifierName(), name)) {
			by_name = i;
		}
	}
	return by_name;
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
	const auto &columns = scan->bind_data->Cast<MultiFileBindData>().initial_reader->GetColumns();
	for (auto &field : bind_data.file_columns) {
		scan->column_indexes.push_back(FindColumn(columns, field.first, field.second));
	}
	if (!bind_data.partition_fields.empty() && scan->column_indexes[0].IsValid()) {
		auto &partition = columns[scan->column_indexes[0].GetIndex()];
		for (auto &field : bind_data.partition_fields) {
			scan->partition_indexes.push_back(
			    FindColumn(partition.children, field.field.get().partition_field_id, field.name));
		}
	}

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

//! A field absent from an older partition struct is NULL in the unified struct.
static Value UnifiedPartitionValue(const Value &file_partition, const vector<IcebergPartitionStructField> &fields,
                                   const vector<optional_idx> &indexes) {
	child_list_t<Value> children;
	for (idx_t i = 0; i < fields.size(); i++) {
		auto &field = fields[i];
		Value value(field.type);
		if (!file_partition.IsNull() && file_partition.type().id() == LogicalTypeId::STRUCT && indexes[i].IsValid()) {
			value = StructValue::GetChildren(file_partition)[indexes[i].GetIndex()].DefaultCastAs(field.type);
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
			auto path = resolved.ResolveStatisticsPath(file.path, options, fs);
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
	auto add_file_column = [&](int32_t id, const char *name, const LogicalType &type) {
		bind_data->file_columns.emplace_back(id, name);
		names.emplace_back(name);
		return_types.emplace_back(type);
	};
	//! The field IDs defined by Iceberg's partition statistics file schema
	if (!bind_data->partition_fields.empty()) {
		add_file_column(1, "partition", IcebergPartitionStruct::GetType(bind_data->partition_fields));
	}
	add_file_column(2, "spec_id", LogicalType::INTEGER);
	add_file_column(3, "data_record_count", LogicalType::BIGINT);
	add_file_column(4, "data_file_count", LogicalType::INTEGER);
	add_file_column(5, "total_data_file_size_in_bytes", LogicalType::BIGINT);
	add_file_column(6, "position_delete_record_count", LogicalType::BIGINT);
	add_file_column(7, "position_delete_file_count", LogicalType::INTEGER);
	add_file_column(8, "equality_delete_record_count", LogicalType::BIGINT);
	add_file_column(9, "equality_delete_file_count", LogicalType::INTEGER);
	add_file_column(10, "total_record_count", LogicalType::BIGINT);
	add_file_column(11, "last_updated_at", LogicalType::TIMESTAMP_MS);
	add_file_column(12, "last_updated_snapshot_id", LogicalType::BIGINT);
	add_file_column(13, "dv_count", LogicalType::INTEGER);
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
		idx_t file_col = 0;
		if (!bind_data.partition_fields.empty()) {
			auto partition_column = scan.column_indexes[file_col++];
			Value partition(LogicalType::SQLNULL);
			if (partition_column.IsValid()) {
				partition = scan.chunk.data[partition_column.GetIndex()].GetValue(row);
			}
			output.data[col++].SetValue(
			    out, UnifiedPartitionValue(partition, bind_data.partition_fields, scan.partition_indexes));
		}
		for (; file_col < scan.column_indexes.size(); file_col++, col++) {
			auto &type = output.data[col].GetType();
			auto file_column = scan.column_indexes[file_col];
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
