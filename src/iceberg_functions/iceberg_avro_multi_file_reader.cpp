#include "iceberg_avro_multi_file_reader.hpp"
#include "iceberg_avro_multi_file_list.hpp"
#include "duckdb/common/exception.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_manifest.hpp"
#include "iceberg_utils.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {
constexpr column_t IcebergAvroMultiFileReader::PARTITION_SPEC_ID_FIELD_ID;
constexpr column_t IcebergAvroMultiFileReader::SEQUENCE_NUMBER_FIELD_ID;
constexpr column_t IcebergAvroMultiFileReader::MANIFEST_FILE_PATH_FIELD_ID;

unique_ptr<MultiFileReader> IcebergAvroMultiFileReader::CreateInstance(const TableFunction &table) {
	return make_uniq<IcebergAvroMultiFileReader>(table.function_info);
}

namespace manifest_list {

static MultiFileColumnDefinition CreateManifestFilePartitionsColumn() {
	MultiFileColumnDefinition partitions("partitions", IcebergManifestList::FieldSummaryType());
	partitions.identifier = Value::INTEGER(PARTITIONS);
	partitions.default_expression = make_uniq<ConstantExpression>(Value(partitions.type));

	MultiFileColumnDefinition field_summary("field_summary", ListType::GetChildType(partitions.type));
	field_summary.identifier = Value::INTEGER(PARTITIONS_ELEMENT);

	//! contains_null - required
	field_summary.children.emplace_back("contains_null", LogicalType::BOOLEAN);
	field_summary.children.back().identifier = Value::INTEGER(FIELD_SUMMARY_CONTAINS_NULL);

	//! contains_nan - optional
	field_summary.children.emplace_back("contains_nan", LogicalType::BOOLEAN);
	field_summary.children.back().identifier = Value::INTEGER(FIELD_SUMMARY_CONTAINS_NAN);
	field_summary.children.back().default_expression = make_uniq<ConstantExpression>(Value(LogicalType::BOOLEAN));

	//! lower_bound - optional
	field_summary.children.emplace_back("lower_bound", LogicalType::BLOB);
	field_summary.children.back().identifier = Value::INTEGER(FIELD_SUMMARY_LOWER_BOUND);
	field_summary.children.back().default_expression = make_uniq<ConstantExpression>(Value(LogicalType::BLOB));

	//! upper_bound - optional
	field_summary.children.emplace_back("upper_bound", LogicalType::BLOB);
	field_summary.children.back().identifier = Value::INTEGER(FIELD_SUMMARY_UPPER_BOUND);
	field_summary.children.back().default_expression = make_uniq<ConstantExpression>(Value(LogicalType::BLOB));

	partitions.children.push_back(field_summary);
	return partitions;
}

static vector<MultiFileColumnDefinition> BuildManifestListSchema(const IcebergTableMetadata &metadata) {
	vector<MultiFileColumnDefinition> schema;

	auto &iceberg_version = metadata.iceberg_version;
	// manifest_path (required)
	MultiFileColumnDefinition manifest_path("manifest_path", LogicalType::VARCHAR);
	manifest_path.identifier = Value::INTEGER(MANIFEST_PATH);
	schema.push_back(manifest_path);

	// manifest_length (required)
	MultiFileColumnDefinition manifest_length("manifest_length", LogicalType::BIGINT);
	manifest_length.identifier = Value::INTEGER(MANIFEST_LENGTH);
	schema.push_back(manifest_length);

	// partition_spec_id (optional, default 0)
	MultiFileColumnDefinition partition_spec_id("partition_spec_id", LogicalType::INTEGER);
	partition_spec_id.identifier = Value::INTEGER(PARTITION_SPEC_ID);
	schema.push_back(partition_spec_id);

	// content (v2+, default 0)
	if (iceberg_version >= 2) {
		MultiFileColumnDefinition content("content", LogicalType::INTEGER);
		content.identifier = Value::INTEGER(CONTENT);
		content.default_expression = make_uniq<ConstantExpression>(Value::INTEGER(0));
		schema.push_back(content);
	}

	// sequence_number (v2+)
	if (iceberg_version >= 2) {
		MultiFileColumnDefinition sequence_number("sequence_number", LogicalType::BIGINT);
		sequence_number.identifier = Value::INTEGER(SEQUENCE_NUMBER);
		sequence_number.default_expression = make_uniq<ConstantExpression>(Value(sequence_number.type));
		schema.push_back(sequence_number);
	}

	// min_sequence_number (v2+, default 0)
	if (iceberg_version >= 2) {
		MultiFileColumnDefinition min_sequence_number("min_sequence_number", LogicalType::BIGINT);
		min_sequence_number.identifier = Value::INTEGER(MIN_SEQUENCE_NUMBER);
		min_sequence_number.default_expression = make_uniq<ConstantExpression>(Value::BIGINT(0));
		schema.push_back(min_sequence_number);
	}

	// added_snapshot_id
	MultiFileColumnDefinition added_snapshot_id("added_snapshot_id", LogicalType::BIGINT);
	added_snapshot_id.identifier = Value::INTEGER(ADDED_SNAPSHOT_ID);
	schema.push_back(added_snapshot_id);

	// added_files_count (v2+)
	MultiFileColumnDefinition added_files_count("added_files_count", LogicalType::INTEGER);
	added_files_count.identifier = Value::INTEGER(ADDED_FILES_COUNT);
	schema.push_back(added_files_count);

	// existing_files_count (v2+)
	MultiFileColumnDefinition existing_files_count("existing_files_count", LogicalType::INTEGER);
	existing_files_count.identifier = Value::INTEGER(EXISTING_FILES_COUNT);
	schema.push_back(existing_files_count);

	// deleted_files_count (v2+)
	MultiFileColumnDefinition deleted_files_count("deleted_files_count", LogicalType::INTEGER);
	deleted_files_count.identifier = Value::INTEGER(DELETED_FILES_COUNT);
	schema.push_back(deleted_files_count);

	// added_rows_count (v2+)
	MultiFileColumnDefinition added_rows_count("added_rows_count", LogicalType::BIGINT);
	added_rows_count.identifier = Value::INTEGER(ADDED_ROWS_COUNT);
	schema.push_back(added_rows_count);

	// existing_rows_count (v2+)
	MultiFileColumnDefinition existing_rows_count("existing_rows_count", LogicalType::BIGINT);
	existing_rows_count.identifier = Value::INTEGER(EXISTING_ROWS_COUNT);
	schema.push_back(existing_rows_count);

	// deleted_rows_count (v2+)
	MultiFileColumnDefinition deleted_rows_count("deleted_rows_count", LogicalType::BIGINT);
	deleted_rows_count.identifier = Value::INTEGER(DELETED_ROWS_COUNT);
	schema.push_back(deleted_rows_count);

	// partitions (v2+)
	schema.push_back(CreateManifestFilePartitionsColumn());

	// first_row_id (v3+, default 0)
	if (iceberg_version >= 3) {
		MultiFileColumnDefinition first_row_id("first_row_id", LogicalType::BIGINT);
		first_row_id.identifier = Value::INTEGER(FIRST_ROW_ID);
		first_row_id.default_expression = make_uniq<ConstantExpression>(Value(first_row_id.type));
		schema.push_back(first_row_id);
	}
	return schema;
}

} // namespace manifest_list

namespace manifest_file {

static MultiFileColumnDefinition CreateManifestPartitionColumn(const map<idx_t, LogicalType> partition_field_id_to_type,
                                                               const LogicalType &partition_type) {
	MultiFileColumnDefinition partition("partition", partition_type);
	partition.identifier = Value::INTEGER(PARTITION);
	for (auto &it : partition_field_id_to_type) {
		auto partition_field_id = it.first;
		auto &type = it.second;

		MultiFileColumnDefinition partition_field(StringUtil::Format("r%d", partition_field_id), type);
		partition_field.identifier = Value::INTEGER(partition_field_id);
		partition_field.default_expression = make_uniq<ConstantExpression>(Value(type));
		partition.children.push_back(partition_field);
	}
	return partition;
}

static vector<MultiFileColumnDefinition>
BuildManifestSchema(const IcebergSnapshot &snapshot, const IcebergTableMetadata &metadata,
                    const map<idx_t, LogicalType> &partition_field_id_to_type) {
	vector<MultiFileColumnDefinition> schema;

	auto &iceberg_version = metadata.iceberg_version;
	// status (required)
	MultiFileColumnDefinition status("status", LogicalType::INTEGER);
	status.identifier = Value::INTEGER(STATUS);
	schema.push_back(status);

	// snapshot_id (optional)
	MultiFileColumnDefinition snapshot_id("snapshot_id", LogicalType::BIGINT);
	snapshot_id.identifier = Value::INTEGER(SNAPSHOT_ID);
	snapshot_id.default_expression = make_uniq<ConstantExpression>(Value(snapshot_id.type));
	schema.push_back(snapshot_id);

	// sequence_number (optional)
	MultiFileColumnDefinition sequence_number("sequence_number", LogicalType::BIGINT);
	sequence_number.identifier = Value::INTEGER(SEQUENCE_NUMBER);
	sequence_number.default_expression = make_uniq<ConstantExpression>(Value(sequence_number.type));
	schema.push_back(sequence_number);

	// file_sequence_number (optional)
	MultiFileColumnDefinition file_sequence_number("file_sequence_number", LogicalType::BIGINT);
	file_sequence_number.identifier = Value::INTEGER(FILE_SEQUENCE_NUMBER);
	file_sequence_number.default_expression = make_uniq<ConstantExpression>(Value(file_sequence_number.type));
	schema.push_back(file_sequence_number);

	//! Map all the referenced partition spec ids to the partition fields that *could* be referenced,
	//! any missing fields will be NULL
	auto partition_type = IcebergDataFile::PartitionStructType(partition_field_id_to_type);

	// data_file
	MultiFileColumnDefinition data_file("data_file", IcebergDataFile::GetType(metadata, partition_type));
	data_file.identifier = Value::INTEGER(DATA_FILE);

	// Add children with their field IDs
	if (iceberg_version >= 2) {
		MultiFileColumnDefinition content("content", LogicalType::INTEGER);
		content.identifier = Value::INTEGER(CONTENT);
		data_file.children.push_back(content);
	}

	MultiFileColumnDefinition file_path("file_path", LogicalType::VARCHAR);
	file_path.identifier = Value::INTEGER(FILE_PATH);
	data_file.children.push_back(file_path);

	MultiFileColumnDefinition file_format("file_format", LogicalType::VARCHAR);
	file_format.identifier = Value::INTEGER(FILE_FORMAT);
	data_file.children.push_back(file_format);

	data_file.children.push_back(CreateManifestPartitionColumn(partition_field_id_to_type, partition_type));

	MultiFileColumnDefinition record_count("record_count", LogicalType::BIGINT);
	record_count.identifier = Value::INTEGER(RECORD_COUNT);
	data_file.children.push_back(record_count);

	MultiFileColumnDefinition file_size_in_bytes("file_size_in_bytes", LogicalType::BIGINT);
	file_size_in_bytes.identifier = Value::INTEGER(FILE_SIZE_IN_BYTES);
	data_file.children.push_back(file_size_in_bytes);

	// column_sizes
	auto column_sizes_type = LogicalType::MAP(LogicalType::INTEGER, LogicalType::BIGINT);
	MultiFileColumnDefinition column_sizes("column_sizes", column_sizes_type);
	column_sizes.identifier = Value::INTEGER(COLUMN_SIZES);
	column_sizes.default_expression = make_uniq<ConstantExpression>(Value(column_sizes_type));
	{
		MultiFileColumnDefinition key("key", MapType::KeyType(column_sizes_type));
		key.identifier = Value::INTEGER(COLUMN_SIZES_KEY);
		column_sizes.children.push_back(key);

		MultiFileColumnDefinition value("value", MapType::ValueType(column_sizes_type));
		value.identifier = Value::INTEGER(COLUMN_SIZES_VALUE);
		column_sizes.children.push_back(value);
	}
	data_file.children.push_back(column_sizes);

	// value_counts
	auto value_counts_type = LogicalType::MAP(LogicalType::INTEGER, LogicalType::BIGINT);
	MultiFileColumnDefinition value_counts("value_counts", value_counts_type);
	value_counts.identifier = Value::INTEGER(VALUE_COUNTS);
	value_counts.default_expression = make_uniq<ConstantExpression>(Value(value_counts_type));
	{
		MultiFileColumnDefinition key("key", MapType::KeyType(value_counts_type));
		key.identifier = Value::INTEGER(VALUE_COUNTS_KEY);
		value_counts.children.push_back(key);

		MultiFileColumnDefinition value("value", MapType::ValueType(value_counts_type));
		value.identifier = Value::INTEGER(VALUE_COUNTS_VALUE);
		value_counts.children.push_back(value);
	}
	data_file.children.push_back(value_counts);

	// null_value_counts
	auto null_value_counts_type = LogicalType::MAP(LogicalType::INTEGER, LogicalType::BIGINT);
	MultiFileColumnDefinition null_value_counts("null_value_counts", null_value_counts_type);
	null_value_counts.identifier = Value::INTEGER(NULL_VALUE_COUNTS);
	null_value_counts.default_expression = make_uniq<ConstantExpression>(Value(null_value_counts_type));
	{
		MultiFileColumnDefinition key("key", MapType::KeyType(null_value_counts_type));
		key.identifier = Value::INTEGER(NULL_VALUE_COUNTS_KEY);
		null_value_counts.children.push_back(key);

		MultiFileColumnDefinition value("value", MapType::ValueType(null_value_counts_type));
		value.identifier = Value::INTEGER(NULL_VALUE_COUNTS_VALUE);
		null_value_counts.children.push_back(value);
	}
	data_file.children.push_back(null_value_counts);

	// nan_value_counts
	auto nan_value_counts_type = LogicalType::MAP(LogicalType::INTEGER, LogicalType::BIGINT);
	MultiFileColumnDefinition nan_value_counts("nan_value_counts", nan_value_counts_type);
	nan_value_counts.identifier = Value::INTEGER(NAN_VALUE_COUNTS);
	nan_value_counts.default_expression = make_uniq<ConstantExpression>(Value(nan_value_counts_type));
	{
		MultiFileColumnDefinition key("key", MapType::KeyType(nan_value_counts_type));
		key.identifier = Value::INTEGER(NAN_VALUE_COUNTS_KEY);
		nan_value_counts.children.push_back(key);

		MultiFileColumnDefinition value("value", MapType::ValueType(nan_value_counts_type));
		value.identifier = Value::INTEGER(NAN_VALUE_COUNTS_VALUE);
		nan_value_counts.children.push_back(value);
	}
	data_file.children.push_back(nan_value_counts);

	// lower_bounds
	auto lower_bounds_type = LogicalType::MAP(LogicalType::INTEGER, LogicalType::BLOB);
	MultiFileColumnDefinition lower_bounds("lower_bounds", lower_bounds_type);
	lower_bounds.identifier = Value::INTEGER(LOWER_BOUNDS);
	lower_bounds.default_expression = make_uniq<ConstantExpression>(Value(lower_bounds_type));
	{
		MultiFileColumnDefinition key("key", MapType::KeyType(lower_bounds_type));
		key.identifier = Value::INTEGER(LOWER_BOUNDS_KEY);
		lower_bounds.children.push_back(key);

		MultiFileColumnDefinition value("value", MapType::ValueType(lower_bounds_type));
		value.identifier = Value::INTEGER(LOWER_BOUNDS_VALUE);
		lower_bounds.children.push_back(value);
	}
	data_file.children.push_back(lower_bounds);

	// upper_bounds
	auto upper_bounds_type = LogicalType::MAP(LogicalType::INTEGER, LogicalType::BLOB);
	MultiFileColumnDefinition upper_bounds("upper_bounds", upper_bounds_type);
	upper_bounds.identifier = Value::INTEGER(UPPER_BOUNDS);
	upper_bounds.default_expression = make_uniq<ConstantExpression>(Value(upper_bounds_type));
	{
		MultiFileColumnDefinition key("key", MapType::KeyType(upper_bounds_type));
		key.identifier = Value::INTEGER(UPPER_BOUNDS_KEY);
		upper_bounds.children.push_back(key);

		MultiFileColumnDefinition value("value", MapType::ValueType(upper_bounds_type));
		value.identifier = Value::INTEGER(UPPER_BOUNDS_VALUE);
		upper_bounds.children.push_back(value);
	}
	data_file.children.push_back(upper_bounds);

	// split_offsets - optional
	auto split_offsets_type = LogicalType::LIST(LogicalType::BIGINT);
	MultiFileColumnDefinition split_offsets("split_offsets", split_offsets_type);
	split_offsets.identifier = Value::INTEGER(SPLIT_OFFSETS);
	split_offsets.default_expression = make_uniq<ConstantExpression>(Value(split_offsets.type));
	{
		MultiFileColumnDefinition list("list", ListType::GetChildType(split_offsets_type));
		list.identifier = Value::INTEGER(SPLIT_OFFSETS_ELEMENT);
		split_offsets.children.push_back(list);
	}
	data_file.children.push_back(split_offsets);

	// equality_ids - optional
	auto equality_ids_type = LogicalType::LIST(LogicalType::INTEGER);
	MultiFileColumnDefinition equality_ids("equality_ids", equality_ids_type);
	equality_ids.identifier = Value::INTEGER(EQUALITY_IDS);
	equality_ids.default_expression = make_uniq<ConstantExpression>(Value(equality_ids.type));
	{
		MultiFileColumnDefinition list("list", ListType::GetChildType(equality_ids_type));
		list.identifier = Value::INTEGER(EQUALITY_IDS_ELEMENT);
		equality_ids.children.push_back(list);
	}
	data_file.children.push_back(equality_ids);

	// sort_order_id - optional
	MultiFileColumnDefinition sort_order_id("sort_order_id", LogicalType::INTEGER);
	sort_order_id.identifier = Value::INTEGER(SORT_ORDER_ID);
	sort_order_id.default_expression = make_uniq<ConstantExpression>(Value(sort_order_id.type));
	data_file.children.push_back(sort_order_id);

	// first_row_id
	if (iceberg_version >= 3) {
		MultiFileColumnDefinition first_row_id("first_row_id", LogicalType::BIGINT);
		first_row_id.identifier = Value::INTEGER(FIRST_ROW_ID);
		first_row_id.default_expression = make_uniq<ConstantExpression>(Value(first_row_id.type));
		data_file.children.push_back(first_row_id);
	}

	// referenced_data_file - optional
	if (iceberg_version >= 2) {
		MultiFileColumnDefinition referenced_data_file("referenced_data_file", LogicalType::VARCHAR);
		referenced_data_file.identifier = Value::INTEGER(REFERENCED_DATA_FILE);
		referenced_data_file.default_expression = make_uniq<ConstantExpression>(Value(referenced_data_file.type));
		data_file.children.push_back(referenced_data_file);
	}

	// content_offset - optional
	if (iceberg_version >= 3) {
		MultiFileColumnDefinition content_offset("content_offset", LogicalType::BIGINT);
		content_offset.identifier = Value::INTEGER(CONTENT_OFFSET);
		content_offset.default_expression = make_uniq<ConstantExpression>(Value(content_offset.type));
		data_file.children.push_back(content_offset);
	}

	// content_size_in_bytes - optional
	if (iceberg_version >= 3) {
		MultiFileColumnDefinition content_size_in_bytes("content_size_in_bytes", LogicalType::BIGINT);
		content_size_in_bytes.identifier = Value::INTEGER(CONTENT_SIZE_IN_BYTES);
		content_size_in_bytes.default_expression = make_uniq<ConstantExpression>(Value(content_size_in_bytes.type));
		data_file.children.push_back(content_size_in_bytes);
	}

	schema.push_back(data_file);
	return schema;
}

} // namespace manifest_file

bool IcebergAvroMultiFileReader::Bind(MultiFileOptions &options, MultiFileList &files,
                                      vector<LogicalType> &return_types, vector<string> &names,
                                      MultiFileReaderBindData &bind_data) {
	auto &iceberg_avro_list = dynamic_cast<IcebergAvroMultiFileList &>(files);

	// Determine if we're reading manifest-list or manifest based on context
	auto &scan_info = *iceberg_avro_list.info;
	auto &type = scan_info.type;
	auto &metadata = scan_info.metadata;
	auto &snapshot = scan_info.snapshot;

	// Build the expected schema with field IDs
	vector<MultiFileColumnDefinition> schema;
	if (type == AvroScanInfoType::MANIFEST_LIST) {
		schema = manifest_list::BuildManifestListSchema(metadata);
	} else {
		auto &manifest_file_scan = scan_info.Cast<IcebergManifestFileScanInfo>();
		auto &manifest_files = manifest_file_scan.manifest_files;
		auto &partition_field_id_to_type = manifest_file_scan.partition_field_id_to_type;
		schema = manifest_file::BuildManifestSchema(snapshot, metadata, partition_field_id_to_type);
	}

	// Populate return_types and names from schema
	for (auto &col : schema) {
		return_types.push_back(col.type);
		names.push_back(col.name);
	}

	// Set the schema in bind_data - framework will use this for mapping
	bind_data.schema = std::move(schema);
	bind_data.mapping = MultiFileColumnMappingMode::BY_FIELD_ID;

	return true;
}

void IcebergAvroMultiFileReader::FinalizeChunk(ClientContext &context, const MultiFileBindData &bind_data,
                                               BaseFileReader &reader, const MultiFileReaderData &reader_data,
                                               DataChunk &input_chunk, DataChunk &output_chunk,
                                               ExpressionExecutor &executor,
                                               optional_ptr<MultiFileReaderGlobalState> global_state_p) {
	// Base class finalization first
	MultiFileReader::FinalizeChunk(context, bind_data, reader, reader_data, input_chunk, output_chunk, executor,
	                               global_state_p);

	auto scan_info = shared_ptr_cast<TableFunctionInfo, IcebergAvroScanInfo>(function_info);
	if (scan_info->type == AvroScanInfoType::MANIFEST_LIST) {
		return;
	}
	auto &manifest_scan_info = scan_info->Cast<IcebergManifestFileScanInfo>();
	auto manifest_file_idx = reader.file_list_idx.GetIndex();
	auto &manifest_file = manifest_scan_info.manifest_files[manifest_file_idx];

	idx_t count = output_chunk.size();
	auto &sequence_number_column = output_chunk.data[2];
	sequence_number_column.Flatten(count);
	auto &sequence_number_validity = FlatVector::Validity(sequence_number_column);
	auto sequence_number_data = FlatVector::GetData<int64_t>(sequence_number_column);
	for (idx_t i = 0; i < count; i++) {
		if (sequence_number_validity.RowIsValid(i)) {
			//! Sequence number is explicitly set
			continue;
		}
		sequence_number_validity.SetValid(i);
		sequence_number_data[i] = manifest_file.sequence_number;
	}
	if (scan_info->metadata.iceberg_version < 3) {
		//! No row-lineage applies, just return
		return;
	}
	if (manifest_file.content == IcebergManifestContentType::DELETE) {
		//! No need to inherit first-row-id for DELETE manifests
		return;
	}

	auto &global_state = global_state_p->Cast<IcebergAvroMultiFileReaderGlobalState>();

	auto res = global_state.added_rows_per_manifest.emplace(manifest_file_idx, 0);
	auto &start_row_id = res.first->second;

	//! NOTE: the order of these columns is defined by the order that they are produced in BuildManifestSchema
	//! see `iceberg_avro_multi_file_reader.cpp`
	auto &data_file_column = output_chunk.data[4];
	auto &data_struct_children = StructVector::GetEntries(data_file_column);

	auto &first_row_id_column = *data_struct_children[15];
	first_row_id_column.Flatten(count);

	auto &record_count_column = *data_struct_children[4];
	record_count_column.Flatten(count);

	auto &first_row_id_validity = FlatVector::Validity(first_row_id_column);
	auto first_row_id_data = FlatVector::GetData<int64_t>(first_row_id_column);
	auto record_count_data = FlatVector::GetData<int64_t>(record_count_column);
	for (idx_t i = 0; i < count; i++) {
		if (first_row_id_validity.RowIsValid(i)) {
			//! First row id is explicitly set
			continue;
		}
		first_row_id_validity.SetValid(i);
		D_ASSERT(manifest_file.has_first_row_id);
		first_row_id_data[i] = manifest_file.first_row_id + start_row_id;
		start_row_id += record_count_data[i];
	}
	(void)output_chunk;
	count += 1;
	return;
}

unique_ptr<Expression> IcebergAvroMultiFileReader::GetVirtualColumnExpression(
    ClientContext &context, MultiFileReaderData &reader_data, const vector<MultiFileColumnDefinition> &local_columns,
    idx_t &column_id, const LogicalType &type, MultiFileLocalIndex local_idx,
    optional_ptr<MultiFileColumnDefinition> &global_column_reference) {
	if (column_id == PARTITION_SPEC_ID_FIELD_ID) {
		if (!reader_data.file_to_be_opened.extended_info) {
			throw InternalException("Extended info not found for partition_spec_id column");
		}
		auto &options = reader_data.file_to_be_opened.extended_info->options;
		auto entry = options.find("partition_spec_id");
		if (entry == options.end()) {
			throw InternalException("'partition_spec_id' not set when initializing the FileList");
		}
		return make_uniq<BoundConstantExpression>(entry->second);
	}
	if (column_id == SEQUENCE_NUMBER_FIELD_ID) {
		if (!reader_data.file_to_be_opened.extended_info) {
			throw InternalException("Extended info not found for sequence number column");
		}
		auto &options = reader_data.file_to_be_opened.extended_info->options;
		auto entry = options.find("sequence_number");
		if (entry == options.end()) {
			throw InternalException("'sequence_number' not set when initializing the FileList");
		}
		return make_uniq<BoundConstantExpression>(entry->second);
	}
	if (column_id == MANIFEST_FILE_PATH_FIELD_ID) {
		if (!reader_data.file_to_be_opened.extended_info) {
			throw InternalException("Extended info not found for manifest_file_path column");
		}
		auto &options = reader_data.file_to_be_opened.extended_info->options;
		auto entry = options.find("manifest_file_path");
		if (entry == options.end()) {
			throw InternalException("'manifest_file_path' not set when initializing the FileList");
		}
		return make_uniq<BoundConstantExpression>(entry->second);
	}
	return MultiFileReader::GetVirtualColumnExpression(context, reader_data, local_columns, column_id, type, local_idx,
	                                                   global_column_reference);
}

unique_ptr<MultiFileReaderGlobalState> IcebergAvroMultiFileReader::InitializeGlobalState(
    ClientContext &context, const MultiFileOptions &file_options, const MultiFileReaderBindData &bind_data,
    const MultiFileList &file_list, const vector<MultiFileColumnDefinition> &global_columns,
    const vector<ColumnIndex> &global_column_ids) {
	vector<LogicalType> extra_columns;
	auto res = make_uniq<IcebergAvroMultiFileReaderGlobalState>(extra_columns, file_list);
	return std::move(res);
}

shared_ptr<MultiFileList> IcebergAvroMultiFileReader::CreateFileList(ClientContext &context,
                                                                     const vector<string> &paths,
                                                                     const FileGlobInput &glob_input) {
	auto scan_info = shared_ptr_cast<TableFunctionInfo, IcebergAvroScanInfo>(function_info);
	vector<OpenFileInfo> open_files;

	if (scan_info->type == AvroScanInfoType::MANIFEST_LIST) {
		D_ASSERT(paths.size() == 1);
		open_files.emplace_back(paths[0]);
		auto &file_info = open_files.back();
		file_info.extended_info = make_uniq<ExtendedOpenFileInfo>();
		file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
		file_info.extended_info->options["force_full_download"] = Value::BOOLEAN(true);
		file_info.extended_info->options["etag"] = Value("");
		file_info.extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
	} else {
		auto &manifest_files_scan = scan_info->Cast<IcebergManifestFileScanInfo>();
		auto &manifest_files = manifest_files_scan.manifest_files;
		auto &options = manifest_files_scan.options;
		auto &fs = manifest_files_scan.fs;
		auto &iceberg_path = manifest_files_scan.iceberg_path;
		for (idx_t i = 0; i < manifest_files.size(); i++) {
			auto &manifest = manifest_files[i];
			auto full_path = options.allow_moved_paths
			                     ? IcebergUtils::GetFullPath(iceberg_path, manifest.manifest_path, fs)
			                     : manifest.manifest_path;
			open_files.emplace_back(full_path);
			auto &file_info = open_files.back();
			file_info.extended_info = make_uniq<ExtendedOpenFileInfo>();
			file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
			file_info.extended_info->options["force_full_download"] = Value::BOOLEAN(true);
			file_info.extended_info->options["file_size"] = Value::UBIGINT(manifest.manifest_length);
			file_info.extended_info->options["etag"] = Value("");
			file_info.extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
			file_info.extended_info->options["partition_spec_id"] = Value::INTEGER(manifest.partition_spec_id);
			file_info.extended_info->options["sequence_number"] = Value::BIGINT(manifest.sequence_number);
			file_info.extended_info->options["manifest_file_path"] = Value(manifest.manifest_path);
		}
	}

	auto res = make_uniq<IcebergAvroMultiFileList>(scan_info, std::move(open_files));
	return std::move(res);
}

} // namespace duckdb
