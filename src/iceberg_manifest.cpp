#include "metadata/iceberg_manifest.hpp"
#include "storage/irc_table_set.hpp"

#include "duckdb/storage/caching_file_system.hpp"
#include "catalog_utils.hpp"
#include "iceberg_value.hpp"
#include "storage/iceberg_table_information.hpp"

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
	if (partition_values.empty()) {
		//! NOTE: Spark does *not* like it when this column is NULL, so we populate it with an empty struct value
		//! instead
		children.push_back(
		    Value::STRUCT(child_list_t<Value> {{"__duckdb_empty_struct_marker", Value(LogicalTypeId::VARCHAR)}}));
	} else {
		child_list_t<Value> partition_children;
		for (auto &field : partition_values) {
			partition_children.emplace_back(StringUtil::Format("r%d", field.first), field.second);
		}
		children.push_back(Value::STRUCT(partition_children));
	}

	// record_count: long - 103
	children.push_back(Value::BIGINT(record_count));
	// file_size_in_bytes: long - 104
	children.push_back(Value::BIGINT(file_size_in_bytes));

	child_list_t<LogicalType> bounds_types;
	bounds_types.emplace_back("key", LogicalType::INTEGER);
	bounds_types.emplace_back("value", LogicalType::BLOB);

	vector<Value> lower_bounds_values;
	// lower bounds: map<126: int, 127: binary> - 125
	for (auto &child : lower_bounds) {
		auto tmp = LogicalType(LogicalTypeId::INTEGER);
		auto tmp2 = child.second.ToString();
		auto serialized_lower_bound = IcebergValue::SerializeValue(child.second, tmp);
		if (serialized_lower_bound.HasError()) {
			throw serialized_lower_bound.GetError();
		}
		lower_bounds_values.push_back(Value::STRUCT({{"key", child.first}, {"value", serialized_lower_bound.value}}));
	}
	children.push_back(Value::MAP(LogicalType::STRUCT(bounds_types), lower_bounds_values));

	vector<Value> upper_bounds_values;
	// upper bounds: map<129: int, 130: binary> - 128
	for (auto &child : upper_bounds) {
		auto tmp = LogicalType(LogicalTypeId::INTEGER);
		;
		auto serialized_upper_bound = IcebergValue::SerializeValue(child.second, tmp);
		if (serialized_upper_bound.HasError()) {
			throw serialized_upper_bound.GetError();
		}
		upper_bounds_values.push_back(Value::STRUCT({{"key", child.first}, {"value", serialized_upper_bound.value}}));
	}
	children.push_back(Value::MAP(LogicalType::STRUCT(bounds_types), upper_bounds_values));

	return Value::STRUCT(type, children);
}

namespace manifest_file {

static LogicalType PartitionStructType(IcebergTableInformation &table_info, const IcebergManifestFile &file) {
	D_ASSERT(!file.data_files.empty());

	auto &first_entry = file.data_files.front();
	child_list_t<LogicalType> children;
	if (first_entry.partition_values.empty()) {
		children.emplace_back("__duckdb_empty_struct_marker", LogicalType::INTEGER);
	} else {
		//! NOTE: all entries in the file should have the same schema, otherwise it can't be in the same manifest file
		//! anyways
		for (auto &it : first_entry.partition_values) {
			children.emplace_back(StringUtil::Format("r%d", it.first), it.second.type());
		}
	}
	return LogicalType::STRUCT(children);
}

idx_t WriteToFile(IcebergTableInformation &table_info, const IcebergManifestFile &manifest_file, CopyFunction &copy,
                  DatabaseInstance &db, ClientContext &context) {
	D_ASSERT(!manifest_file.data_files.empty());
	auto &allocator = db.GetBufferManager().GetBufferAllocator();

	//! We need to create an iceberg-schema for the manifest file, written in the metadata of the Avro file.
	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
	auto doc = doc_p.get();
	auto root_obj = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root_obj);
	yyjson_mut_obj_add_strcpy(doc, root_obj, "type", "struct");
	yyjson_mut_obj_add_uint(doc, root_obj, "schema-id", 0);
	auto fields_arr = yyjson_mut_obj_add_arr(doc, root_obj, "fields");

	//! Create the types for the DataChunk

	child_list_t<Value> field_ids;
	vector<string> names;
	vector<LogicalType> types;

	auto &current_partition_spec = table_info.table_metadata.GetLatestPartitionSpec();

	{
		child_list_t<Value> status_field;
		// status: int - 0
		names.push_back("status");
		types.push_back(LogicalType::INTEGER);
		status_field.emplace_back("__duckdb_field_id", Value::INTEGER(STATUS));
		status_field.emplace_back("__duckdb_nullable", Value::BOOLEAN(false));

		auto field_obj = yyjson_mut_arr_add_obj(doc, fields_arr);
		yyjson_mut_obj_add_uint(doc, field_obj, "id", STATUS);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "name", "status");
		yyjson_mut_obj_add_bool(doc, field_obj, "required", true);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type", "int");
		field_ids.emplace_back("status", Value::STRUCT(status_field));
	}

	{
		// snapshot_id: long - 1
		names.push_back("snapshot_id");
		types.push_back(LogicalType::BIGINT);
		field_ids.emplace_back("snapshot_id", Value::INTEGER(SNAPSHOT_ID));

		auto field_obj = yyjson_mut_arr_add_obj(doc, fields_arr);
		yyjson_mut_obj_add_uint(doc, field_obj, "id", SNAPSHOT_ID);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "name", "snapshot_id");
		yyjson_mut_obj_add_bool(doc, field_obj, "required", false);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type", "long");
	}

	{
		// sequence_number: long - 3
		names.push_back("sequence_number");
		types.push_back(LogicalType::BIGINT);
		field_ids.emplace_back("sequence_number", Value::INTEGER(SEQUENCE_NUMBER));

		auto field_obj = yyjson_mut_arr_add_obj(doc, fields_arr);
		yyjson_mut_obj_add_uint(doc, field_obj, "id", SEQUENCE_NUMBER);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "name", "sequence_number");
		yyjson_mut_obj_add_bool(doc, field_obj, "required", false);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type", "long");
	}

	{
		// file_sequence_number: long - 4
		names.push_back("file_sequence_number");
		types.push_back(LogicalType::BIGINT);
		field_ids.emplace_back("file_sequence_number", Value::INTEGER(FILE_SEQUENCE_NUMBER));

		auto field_obj = yyjson_mut_arr_add_obj(doc, fields_arr);
		yyjson_mut_obj_add_uint(doc, field_obj, "id", FILE_SEQUENCE_NUMBER);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "name", "file_sequence_number");
		yyjson_mut_obj_add_bool(doc, field_obj, "required", false);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type", "long");
	}

	//! DataFile struct

	child_list_t<Value> data_file_field_ids;
	child_list_t<LogicalType> children;

	auto child_fields_arr = yyjson_mut_arr(doc);
	{
		child_list_t<Value> content_field;
		// content: int - 134
		children.emplace_back("content", LogicalType::INTEGER);
		content_field.emplace_back("__duckdb_field_id", Value::INTEGER(CONTENT));
		content_field.emplace_back("__duckdb_nullable", Value::BOOLEAN(false));
		data_file_field_ids.emplace_back("content", Value::STRUCT(content_field));

		auto field_obj = yyjson_mut_arr_add_obj(doc, child_fields_arr);
		yyjson_mut_obj_add_uint(doc, field_obj, "id", CONTENT);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "name", "content");
		yyjson_mut_obj_add_bool(doc, field_obj, "required", true);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type", "int");
	}

	{
		child_list_t<Value> file_path;
		// file_path: string - 100
		children.emplace_back("file_path", LogicalType::VARCHAR);
		file_path.emplace_back("__duckdb_field_id", Value::INTEGER(FILE_PATH));
		file_path.emplace_back("__duckdb_nullable", Value::BOOLEAN(false));
		data_file_field_ids.emplace_back("file_path", Value::STRUCT(file_path));

		auto field_obj = yyjson_mut_arr_add_obj(doc, child_fields_arr);
		yyjson_mut_obj_add_uint(doc, field_obj, "id", FILE_PATH);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "name", "file_path");
		yyjson_mut_obj_add_bool(doc, field_obj, "required", true);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type", "string");
	}

	{
		child_list_t<Value> file_format;
		// file_format: string - 101
		children.emplace_back("file_format", LogicalType::VARCHAR);
		file_format.emplace_back("__duckdb_field_id", Value::INTEGER(FILE_FORMAT));
		file_format.emplace_back("__duckdb_nullable", Value::BOOLEAN(false));
		data_file_field_ids.emplace_back("file_format", Value::STRUCT(file_format));

		auto field_obj = yyjson_mut_arr_add_obj(doc, child_fields_arr);
		yyjson_mut_obj_add_uint(doc, field_obj, "id", FILE_FORMAT);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "name", "file_format");
		yyjson_mut_obj_add_bool(doc, field_obj, "required", true);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type", "string");
	}

	{
		child_list_t<Value> partition;
		// partition: struct(...) - 102
		children.emplace_back("partition", PartitionStructType(table_info, manifest_file));
		partition.emplace_back("__duckdb_field_id", Value::INTEGER(PARTITION));
		partition.emplace_back("__duckdb_nullable", Value::BOOLEAN(false));
		data_file_field_ids.emplace_back("partition", Value::STRUCT(partition));

		auto field_obj = yyjson_mut_arr_add_obj(doc, child_fields_arr);
		yyjson_mut_obj_add_uint(doc, field_obj, "id", PARTITION);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "name", "partition");
		yyjson_mut_obj_add_bool(doc, field_obj, "required", true);

		auto partition_struct = yyjson_mut_obj_add_obj(doc, field_obj, "type");
		yyjson_mut_obj_add_strcpy(doc, partition_struct, "type", "struct");
		//! NOTE: this has to be populated with the fields of the partition spec when we support INSERT into a
		//! partitioned table
		[[maybe_unused]] auto partition_fields = yyjson_mut_obj_add_arr(doc, partition_struct, "fields");
	}

	{
		child_list_t<Value> record_count;
		// record_count: long - 103
		children.emplace_back("record_count", LogicalType::BIGINT);
		record_count.emplace_back("__duckdb_field_id", Value::INTEGER(RECORD_COUNT));
		record_count.emplace_back("__duckdb_nullable", Value::BOOLEAN(false));
		data_file_field_ids.emplace_back("record_count", Value::STRUCT(record_count));

		auto field_obj = yyjson_mut_arr_add_obj(doc, child_fields_arr);
		yyjson_mut_obj_add_uint(doc, field_obj, "id", RECORD_COUNT);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "name", "record_count");
		yyjson_mut_obj_add_bool(doc, field_obj, "required", true);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type", "long");
	}

	{
		child_list_t<Value> file_size_in_bytes;
		// file_size_in_bytes: long - 104
		children.emplace_back("file_size_in_bytes", LogicalType::BIGINT);
		file_size_in_bytes.emplace_back("__duckdb_field_id", Value::INTEGER(FILE_SIZE_IN_BYTES));
		file_size_in_bytes.emplace_back("__duckdb_nullable", Value::BOOLEAN(false));
		data_file_field_ids.emplace_back("file_size_in_bytes", Value::STRUCT(file_size_in_bytes));

		auto field_obj = yyjson_mut_arr_add_obj(doc, child_fields_arr);
		yyjson_mut_obj_add_uint(doc, field_obj, "id", FILE_SIZE_IN_BYTES);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "name", "file_size_in_bytes");
		yyjson_mut_obj_add_bool(doc, field_obj, "required", true);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type", "long");
	}

	//! NOTE: These are optional but we should probably add them, to support better filtering
	//! column_sizes
	//! value_counts
	//! null_value_counts
	//! nan_value_counts

	// lower bounds struct
	child_list_t<LogicalType> bounds_fields;
	bounds_fields.emplace_back("key", LogicalType::INTEGER);
	bounds_fields.emplace_back("value", LogicalType::BLOB);
	{
		// child_list_t<Value> lower_bounds_field_ids;
		// lower bounds: map<126: int, 127: binary> - 104
		children.emplace_back("lower_bounds", LogicalType::MAP(LogicalType::STRUCT(bounds_fields)));

		child_list_t<Value> lower_bound_record_field_ids;
		lower_bound_record_field_ids.emplace_back("__duckdb_field_id", Value::INTEGER(LOWER_BOUNDS));
		lower_bound_record_field_ids.emplace_back("key", Value::INTEGER(LOWER_BOUNDS_KEY));
		lower_bound_record_field_ids.emplace_back("value", Value::INTEGER(LOWER_BOUNDS_VALUE));

		data_file_field_ids.emplace_back("lower_bounds", Value::STRUCT(lower_bound_record_field_ids));

		auto field_obj = yyjson_mut_arr_add_obj(doc, child_fields_arr);
		yyjson_mut_obj_add_uint(doc, field_obj, "id", LOWER_BOUNDS);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "name", "lower_bounds");
		yyjson_mut_obj_add_bool(doc, field_obj, "required", false);

		auto lower_bound_type_struct = yyjson_mut_obj_add_obj(doc, field_obj, "type");
		yyjson_mut_obj_add_strcpy(doc, lower_bound_type_struct, "type", "array");
		auto items_obj = yyjson_mut_obj_add_obj(doc, lower_bound_type_struct, "items");
		yyjson_mut_obj_add_strcpy(doc, items_obj, "type", "record");
		yyjson_mut_obj_add_strcpy(doc, items_obj, "name",
		                          StringUtil::Format("k%d_k%d", LOWER_BOUNDS_KEY, LOWER_BOUNDS_VALUE).c_str());
		auto record_fields_arr = yyjson_mut_obj_add_arr(doc, items_obj, "fields");

		auto key_obj = yyjson_mut_arr_add_obj(doc, record_fields_arr);
		yyjson_mut_obj_add_strcpy(doc, key_obj, "name", "key");
		yyjson_mut_obj_add_strcpy(doc, key_obj, "type", "int");
		yyjson_mut_obj_add_uint(doc, key_obj, "id", LOWER_BOUNDS_KEY);

		auto val_obj = yyjson_mut_arr_add_obj(doc, record_fields_arr);
		yyjson_mut_obj_add_strcpy(doc, val_obj, "name", "value");
		yyjson_mut_obj_add_strcpy(doc, val_obj, "type", "binary");
		yyjson_mut_obj_add_uint(doc, val_obj, "id", LOWER_BOUNDS_VALUE);
	}

	// upper bounds struct
	{
		// child_list_t<Value> upper_bounds_field_ids;
		// upper bounds: map<129: int, 130: binary>
		children.emplace_back("upper_bounds", LogicalType::MAP(LogicalType::STRUCT(bounds_fields)));

		child_list_t<Value> upper_bound_record_field_ids;
		upper_bound_record_field_ids.emplace_back("__duckdb_field_id", Value::INTEGER(UPPER_BOUNDS));
		upper_bound_record_field_ids.emplace_back("key", Value::INTEGER(UPPER_BOUNDS_KEY));
		upper_bound_record_field_ids.emplace_back("value", Value::INTEGER(UPPER_BOUNDS_VALUE));

		data_file_field_ids.emplace_back("upper_bounds", Value::STRUCT(upper_bound_record_field_ids));
		auto field_obj = yyjson_mut_arr_add_obj(doc, child_fields_arr);
		yyjson_mut_obj_add_uint(doc, field_obj, "id", UPPER_BOUNDS);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "name", "upper_bounds");
		yyjson_mut_obj_add_bool(doc, field_obj, "required", false);

		auto upper_bound_type_struct = yyjson_mut_obj_add_obj(doc, field_obj, "type");
		yyjson_mut_obj_add_strcpy(doc, upper_bound_type_struct, "type", "array");
		auto items_obj = yyjson_mut_obj_add_obj(doc, upper_bound_type_struct, "items");
		yyjson_mut_obj_add_strcpy(doc, items_obj, "type", "record");
		yyjson_mut_obj_add_strcpy(doc, items_obj, "name",
		                          StringUtil::Format("k%d_k%d", UPPER_BOUNDS_KEY, UPPER_BOUNDS_VALUE).c_str());
		auto record_fields_arr = yyjson_mut_obj_add_arr(doc, items_obj, "fields");

		auto key_obj = yyjson_mut_arr_add_obj(doc, record_fields_arr);
		yyjson_mut_obj_add_strcpy(doc, key_obj, "name", "key");
		yyjson_mut_obj_add_strcpy(doc, key_obj, "type", "int");
		yyjson_mut_obj_add_uint(doc, key_obj, "id", UPPER_BOUNDS_KEY);

		auto val_obj = yyjson_mut_arr_add_obj(doc, record_fields_arr);
		yyjson_mut_obj_add_strcpy(doc, val_obj, "name", "value");
		yyjson_mut_obj_add_strcpy(doc, val_obj, "type", "binary");
		yyjson_mut_obj_add_uint(doc, val_obj, "id", UPPER_BOUNDS_VALUE);
	}

	{
		// data_file: struct(...) - 2
		names.push_back("data_file");
		types.push_back(LogicalType::STRUCT(std::move(children)));
		data_file_field_ids.emplace_back("__duckdb_field_id", Value::INTEGER(DATA_FILE));
		data_file_field_ids.emplace_back("__duckdb_nullable", Value::BOOLEAN(false));
		field_ids.emplace_back("data_file", Value::STRUCT(data_file_field_ids));

		auto field_obj = yyjson_mut_arr_add_obj(doc, fields_arr);
		yyjson_mut_obj_add_uint(doc, field_obj, "id", DATA_FILE);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "name", "data_file");
		yyjson_mut_obj_add_bool(doc, field_obj, "required", true);

		auto data_file_struct = yyjson_mut_obj_add_obj(doc, field_obj, "type");
		yyjson_mut_obj_add_strcpy(doc, data_file_struct, "type", "struct");
		yyjson_mut_obj_add_val(doc, data_file_struct, "fields", child_fields_arr);
	}

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
		data.SetValue(col_idx++, i, Value::BIGINT(data_file.snapshot_id));
		// sequence_number: long - 3
		data.SetValue(col_idx++, i, Value::BIGINT(data_file.sequence_number));
		// file_sequence_number: long - 4
		data.SetValue(col_idx++, i, Value(LogicalType::BIGINT));
		// data_file: struct(...) - 2
		data.SetValue(col_idx, i, data_file.ToDataFileStruct(data.data[col_idx].GetType()));
		col_idx++;
	}
	data.SetCardinality(manifest_file.data_files.size());
	auto iceberg_schema_string = ICUtils::JsonToString(std::move(doc_p));

	child_list_t<Value> metadata_values;
	metadata_values.emplace_back("schema", iceberg_schema_string);
	metadata_values.emplace_back("schema-id", std::to_string(table_info.table_metadata.current_schema_id));
	metadata_values.emplace_back("partition-spec", current_partition_spec.FieldsToJSON());
	metadata_values.emplace_back("partition-spec-id", std::to_string(current_partition_spec.spec_id));
	metadata_values.emplace_back("format-version", std::to_string(table_info.table_metadata.iceberg_version));
	metadata_values.emplace_back("content", "data");
	auto metadata_map = Value::STRUCT(std::move(metadata_values));

	CopyInfo copy_info;
	copy_info.is_from = false;
	copy_info.options["root_name"].push_back(Value("manifest_entry"));
	copy_info.options["field_ids"].push_back(Value::STRUCT(field_ids));
	copy_info.options["metadata"].push_back(metadata_map);

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
