#include "metadata/iceberg_manifest.hpp"
#include "storage/irc_table_set.hpp"

#include "duckdb/storage/caching_file_system.hpp"

namespace duckdb {

Value IcebergManifestEntry::ToDataFileStruct(const LogicalType &type) const {
	vector<Value> children;

	// content: int - 134
	children.push_back(Value::INTEGER(static_cast<int32_t>(content)));
	// file_path: string - 100
	children.push_back(Value(file_path));
	// file_format: string - 101
	children.push_back(Value(file_format));
	// partition: struct(...) - 102
	children.push_back(partition);
	// record_count: long - 103
	children.push_back(Value::BIGINT(record_count));
	// file_size_in_bytes: long - 104
	children.push_back(Value::BIGINT(file_size_in_bytes));

	return Value::STRUCT(type, children);
}

namespace manifest_file {

static LogicalType PartitionStructType(IcebergTableInformation &table_info) {
	//! TODO: actually use the partition info
	child_list_t<LogicalType> children;
	children.emplace_back("dummy", LogicalType::INTEGER);
	return LogicalType::STRUCT(children);
}

idx_t WriteToFile(IcebergTableInformation &table_info, const IcebergManifestFile &manifest_file, CopyFunction &copy,
                  DatabaseInstance &db, ClientContext &context) {
	auto &allocator = db.GetBufferManager().GetBufferAllocator();

	//! Create the types for the DataChunk

	child_list_t<Value> field_ids;
	vector<string> names;
	vector<LogicalType> types;
	// status: int - 0
	names.push_back("status");
	types.push_back(LogicalType::INTEGER);
	field_ids.emplace_back("status", Value::INTEGER(STATUS));

	// snapshot_id: long - 1
	names.push_back("snapshot_id");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("snapshot_id", Value::INTEGER(SNAPSHOT_ID));

	// sequence_number: long - 3
	names.push_back("sequence_number");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("sequence_number", Value::INTEGER(SEQUENCE_NUMBER));

	// file_sequence_number: long - 4
	names.push_back("file_sequence_number");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("file_sequence_number", Value::INTEGER(FILE_SEQUENCE_NUMBER));

	//! DataFile struct

	child_list_t<Value> data_file_field_ids;
	child_list_t<LogicalType> children;
	// content: int - 134
	children.emplace_back("content", LogicalType::INTEGER);
	data_file_field_ids.emplace_back("content", Value::INTEGER(CONTENT));

	// file_path: string - 100
	children.emplace_back("file_path", LogicalType::VARCHAR);
	data_file_field_ids.emplace_back("file_path", Value::INTEGER(FILE_PATH));

	// file_format: string - 101
	children.emplace_back("file_format", LogicalType::VARCHAR);
	data_file_field_ids.emplace_back("file_format", Value::INTEGER(FILE_FORMAT));

	// partition: struct(...) - 102
	children.emplace_back("partition", PartitionStructType(table_info));
	data_file_field_ids.emplace_back("partition", Value::INTEGER(PARTITION));

	// record_count: long - 103
	children.emplace_back("record_count", LogicalType::BIGINT);
	data_file_field_ids.emplace_back("record_count", Value::INTEGER(RECORD_COUNT));

	// file_size_in_bytes: long - 104
	children.emplace_back("file_size_in_bytes", LogicalType::BIGINT);
	data_file_field_ids.emplace_back("file_size_in_bytes", Value::INTEGER(FILE_SIZE_IN_BYTES));

	//// column_sizes: map<117: int, 118: long> - 108
	// children.emplace_back("column_sizes", LogicalType::MAP(LogicalType::INTEGER, LogicalType::BIGINT));
	//// value_counts: map<119: int, 120: long> - 109
	// children.emplace_back("value_counts", LogicalType::MAP(LogicalType::INTEGER, LogicalType::BIGINT));
	//// null_value_counts: map<121: int, 122: long> - 110
	// children.emplace_back("null_value_counts", LogicalType::MAP(LogicalType::INTEGER, LogicalType::BIGINT));
	//// nan_value_counts: map<138: int, 139: long> - 137
	// children.emplace_back("nan_value_counts", LogicalType::MAP(LogicalType::INTEGER, LogicalType::BIGINT));
	//// lower_bounds: map<126: int, 127: binary> - 125
	// children.emplace_back("lower_bounds", LogicalType::MAP(LogicalType::INTEGER, LogicalType::BLOB));
	//// upper_bounds: map<129: int, 130: binary> - 128
	// children.emplace_back("upper_bounds", LogicalType::MAP(LogicalType::INTEGER, LogicalType::BLOB));

	// data_file: struct(...) - 2
	names.push_back("data_file");
	types.push_back(LogicalType::STRUCT(std::move(children)));
	data_file_field_ids.emplace_back("__duckdb_field_id", Value::INTEGER(DATA_FILE));
	field_ids.emplace_back("data_file", Value::STRUCT(data_file_field_ids));

	//! Populate the DataChunk with the data files

	DataChunk data;
	data.Initialize(allocator, types, manifest_file.data_files.size());

	for (idx_t i = 0; i < manifest_file.data_files.size(); i++) {
		auto &data_file = manifest_file.data_files[i];
		idx_t col_idx = 0;

		//! We rely on inheriting the snapshot_id, this is only acceptable for ADDED data files
		D_ASSERT(data_file.status == IcebergManifestEntryStatusType::ADDED);

		// status: int - 0
		data.SetValue(col_idx++, i, Value::INTEGER(static_cast<int32_t>(data_file.status)));
		// snapshot_id: long - 1
		data.SetValue(col_idx++, i, Value(LogicalType::BIGINT));
		// sequence_number: long - 3
		data.SetValue(col_idx++, i, Value(LogicalType::BIGINT));
		// file_sequence_number: long - 4
		data.SetValue(col_idx++, i, Value(LogicalType::BIGINT));
		// data_file: struct(...) - 2
		data.SetValue(col_idx, i, data_file.ToDataFileStruct(data.data[col_idx].GetType()));
		col_idx++;
	}
	data.SetCardinality(manifest_file.data_files.size());

	CopyInfo copy_info;
	copy_info.is_from = false;
	copy_info.options["root_name"].push_back(Value("manifest_entry"));
	copy_info.options["field_ids"].push_back(Value::STRUCT(field_ids));

	CopyFunctionBindInput input(copy_info);
	input.file_extension = "avro";

	{
		ThreadContext thread_context(context);
		ExecutionContext execution_context(context, thread_context, nullptr);
		auto bind_data = copy.copy_to_bind(context, input, names, types);

		auto global_state = copy.copy_to_initialize_global(context, *bind_data, manifest_file.path);
		auto local_state = copy.copy_to_initialize_local(execution_context, *bind_data);

		copy.copy_to_sink(execution_context, *bind_data, *global_state, *local_state, data);
		copy.copy_to_combine(execution_context, *bind_data, *global_state, *local_state);
		copy.copy_to_finalize(context, *bind_data, *global_state);
	}

	auto file_system = CachingFileSystem::Get(context);
	auto file_handle = file_system.OpenFile(manifest_file.path, FileOpenFlags::FILE_FLAGS_READ);
	auto manifest_length = file_handle->GetFileSize();
	return manifest_length;
}

} // namespace manifest_file

} // namespace duckdb
