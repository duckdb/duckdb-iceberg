#include "function/scan_planning/iceberg_scan_task_codec.hpp"
#include "core/metadata/partition/iceberg_partition_constants.hpp"

namespace duckdb {

LogicalType IcebergScanTaskCodec::DeleteFileType() {
	return LogicalType::STRUCT({{"file_path", LogicalType::VARCHAR},
	                            {"file_format", LogicalType::VARCHAR},
	                            {"content", LogicalType::INTEGER},
	                            {"file_size_in_bytes", LogicalType::BIGINT},
	                            {"record_count", LogicalType::BIGINT},
	                            {"equality_ids", LogicalType::LIST(LogicalType::INTEGER)},
	                            {"referenced_data_file", LogicalType::VARCHAR},
	                            {"content_offset", LogicalType::BIGINT},
	                            {"content_size_in_bytes", LogicalType::BIGINT}});
}

LogicalType IcebergScanTaskCodec::SchemaType(const IcebergTableSchema &schema) {
	vector<Identifier> names;
	for (auto &column : schema.columns) {
		names.emplace_back(column->name);
	}
	QueryResult::DeduplicateColumns(names);
	child_list_t<LogicalType> children;
	for (idx_t i = 0; i < names.size(); i++) {
		children.emplace_back(names[i].GetIdentifierName(), schema.columns[i]->type);
	}
	return LogicalType::STRUCT(std::move(children));
}

child_list_t<LogicalType> IcebergScanTaskCodec::Columns(const LogicalType &partition_type,
                                                        const LogicalType &schema_type) {
	return {{"file_path", LogicalType::VARCHAR},
	        {"file_format", LogicalType::VARCHAR},
	        {"file_size_in_bytes", LogicalType::BIGINT},
	        {"record_count", LogicalType::BIGINT},
	        {"sequence_number", LogicalType::BIGINT},
	        {"first_row_id", LogicalType::BIGINT},
	        {"partition_spec_id", LogicalType::INTEGER},
	        {"partition_constants", partition_type},
	        {"delete_files", LogicalType::LIST(DeleteFileType())},
	        {"snapshot_id", LogicalType::BIGINT},
	        {"schema_id", LogicalType::INTEGER},
	        {"metadata", LogicalType::VARIANT()},
	        {"schema", schema_type},
	        {"row_filter", LogicalType::VARCHAR}};
}

IcebergDeleteFile IcebergScanTaskCodec::ReadDeleteFile(const Value &descriptor) {
	if (descriptor.IsNull() || descriptor.type() != DeleteFileType()) {
		throw InvalidInputException("iceberg_scan_tasks requires non-NULL delete descriptors of the scan-plan type");
	}
	auto &values = StructValue::GetChildren(descriptor);
	for (idx_t i = 0; i < 6; i++) {
		if (values[i].IsNull()) {
			throw InvalidInputException("iceberg_scan_tasks delete descriptor '%s' cannot be NULL",
			                            StructType::GetChildName(descriptor.type(), i));
		}
	}
	IcebergDeleteFile file;
	file.file_path = StringValue::Get(values[0]);
	file.file_format = StringValue::Get(values[1]);
	auto content = IntegerValue::Get(values[2]);
	if (content != 1 && content != 2) {
		throw InvalidInputException("iceberg_scan_tasks invalid delete content %d", content);
	}
	file.content = static_cast<IcebergManifestEntryContentType>(content);
	file.file_size_in_bytes = BigIntValue::Get(values[3]);
	file.record_count = BigIntValue::Get(values[4]);
	if (file.file_path.empty() || file.file_size_in_bytes < 0 || file.record_count < 0) {
		throw InvalidInputException(
		    "iceberg_scan_tasks delete file requires a path and nonnegative size and record count");
	}
	unordered_set<int32_t> ids;
	for (auto &id : ListValue::GetChildren(values[5])) {
		if (id.IsNull() || IntegerValue::Get(id) <= 0 || !ids.insert(IntegerValue::Get(id)).second) {
			throw InvalidInputException("iceberg_scan_tasks equality_ids must contain distinct positive field IDs");
		}
		file.equality_ids.push_back(IntegerValue::Get(id));
	}
	if (content == 2 && file.equality_ids.empty()) {
		throw InvalidInputException("iceberg_scan_tasks equality delete requires equality_ids");
	}
	if (!values[6].IsNull()) {
		file.referenced_data_file = StringValue::Get(values[6]);
	}
	if (!values[7].IsNull()) {
		file.content_offset = BigIntValue::Get(values[7]);
	}
	if (!values[8].IsNull()) {
		file.content_size_in_bytes = BigIntValue::Get(values[8]);
	}
	if (StringUtil::CIEquals(file.file_format, "puffin")) {
		if (content != 1 || !file.referenced_data_file || file.referenced_data_file->empty() || !file.content_offset ||
		    !file.content_size_in_bytes || *file.content_offset < 0 || *file.content_size_in_bytes < 12 ||
		    *file.content_offset > file.file_size_in_bytes ||
		    *file.content_size_in_bytes > file.file_size_in_bytes - *file.content_offset) {
			throw InvalidInputException("iceberg_scan_tasks invalid Puffin deletion-vector descriptor");
		}
	} else if (!StringUtil::CIEquals(file.file_format, "parquet")) {
		throw NotImplementedException("File format '%s' not supported for deletes", file.file_format);
	}
	return file;
}

IcebergScanTaskCodec::InputLayout
IcebergScanTaskCodec::BindInput(const LogicalType &task_type, vector<LogicalType> &types, vector<Identifier> &names) {
	InputLayout result;
	auto &fields = StructType::GetChildTypes(task_type);
	case_insensitive_map_t<idx_t> indexes;
	for (idx_t i = 0; i < fields.size(); i++) {
		if (!indexes.emplace(fields[i].first.GetIdentifierName(), i).second) {
			throw BinderException("iceberg_scan_tasks input contains duplicate column '%s'", fields[i].first);
		}
	}
	auto expected = Columns(LogicalType::STRUCT({}), LogicalType::STRUCT({}));
	for (idx_t i = 0; i < expected.size(); i++) {
		auto entry = indexes.find(expected[i].first.GetIdentifierName());
		if (entry == indexes.end()) {
			if (i == ROW_FILTER) {
				result.columns.push_back(DConstants::INVALID_INDEX);
				continue;
			}
			throw BinderException("iceberg_scan_tasks missing required input column '%s'",
			                      expected[i].first.GetIdentifierName());
		}
		auto &type = fields[entry->second].second;
		bool valid =
		    i == PARTITION_CONSTANTS || i == SCHEMA ? type.id() == LogicalTypeId::STRUCT : type == expected[i].second;
		if (!valid) {
			throw BinderException("iceberg_scan_tasks input column '%s' has unexpected type %s (expected %s)",
			                      expected[i].first.GetIdentifierName(), type.ToString(),
			                      expected[i].second.ToString());
		}
		result.columns.push_back(entry->second);
	}
	result.schema_type = fields[result.columns[SCHEMA]].second;
	for (auto &column : StructType::GetChildTypes(result.schema_type)) {
		names.emplace_back(column.first);
		types.push_back(column.second);
	}
	if (types.empty()) {
		throw BinderException("iceberg_scan_tasks schema must contain at least one column");
	}
	auto &partition_type = fields[result.columns[PARTITION_CONSTANTS]].second;
	unordered_set<int32_t> field_ids;
	for (auto &field : StructType::GetChildTypes(partition_type)) {
		auto id = Value(field.first).DefaultTryCastAs(LogicalType::INTEGER);
		if (!id || IntegerValue::Get(*id) <= 0 || !field_ids.insert(IntegerValue::Get(*id)).second) {
			throw BinderException("iceberg_scan_tasks partition constant names must be distinct positive field IDs");
		}
		result.partition_ids.push_back(IntegerValue::Get(*id));
	}
	return result;
}

Value IcebergScanTaskCodec::ReadValue(const Value &input, const InputLayout &bind, Column column, bool nullable) {
	if (column == ROW_FILTER && bind.columns[column] == DConstants::INVALID_INDEX) {
		return Value(LogicalType::VARCHAR);
	}
	auto value = StructValue::GetChildren(input)[bind.columns[column]];
	if (!nullable && value.IsNull()) {
		throw InvalidInputException(
		    "iceberg_scan_tasks input column '%s' cannot be NULL",
		    Columns(LogicalType::STRUCT({}), LogicalType::STRUCT({}))[column].first.GetIdentifierName());
	}
	return value;
}

IcebergFileScanTask IcebergScanTaskCodec::ReadTask(const Value &input, const InputLayout &bind,
                                                   const IcebergTableMetadata &metadata,
                                                   const IcebergTableSchema &schema) {
	IcebergFileScanTask result;
	auto path = ReadValue(input, bind, FILE_PATH);
	auto format = ReadValue(input, bind, FILE_FORMAT);
	auto size = ReadValue(input, bind, FILE_SIZE);
	auto count = ReadValue(input, bind, RECORD_COUNT);
	if (BigIntValue::Get(count) < 0) {
		throw InvalidInputException("iceberg_scan_tasks record_count cannot be negative");
	}
	auto spec = ReadValue(input, bind, PARTITION_SPEC_ID);
	if (!metadata.partition_specs.count(IntegerValue::Get(spec))) {
		throw InvalidInputException("iceberg_scan_tasks partition_spec_id is absent from the metadata");
	}
	auto first_row = ReadValue(input, bind, FIRST_ROW_ID, true);
	auto sequence = ReadValue(input, bind, SEQUENCE_NUMBER, true);
	result.file_path = StringValue::Get(path);
	result.original_file_path = result.file_path;
	result.file_format = StringValue::Get(format);
	result.file_size_in_bytes = BigIntValue::Get(size);
	result.record_count = BigIntValue::Get(count);
	result.partition_spec_id = IntegerValue::Get(spec);
	result.first_row_id = first_row.IsNull() ? nullopt : optional<int64_t>(BigIntValue::Get(first_row));
	result.sequence_number = sequence.IsNull() ? nullopt : optional<int64_t>(BigIntValue::Get(sequence));

	auto constants = ReadValue(input, bind, PARTITION_CONSTANTS);
	auto &values = StructValue::GetChildren(constants);
	for (idx_t i = 0; i < values.size(); i++) {
		auto type = IcebergPartitionConstants::GetType(bind.partition_ids[i], schema, metadata.GetSchemas());
		if (!type || *type != values[i].type()) {
			throw InvalidInputException("iceberg_scan_tasks partition constant %d does not match the table schemas",
			                            bind.partition_ids[i]);
		}
		result.partition_constants.emplace(bind.partition_ids[i], values[i]);
	}
	auto deletes = ReadValue(input, bind, DELETE_FILES);
	for (auto &descriptor : ListValue::GetChildren(deletes)) {
		result.delete_files.push_back(ReadDeleteFile(descriptor));
	}

	return result;
}

IcebergTableMetadata IcebergScanTaskCodec::ReadMetadata(const string &text, int32_t schema_id, const Value &snapshot,
                                                        const LogicalType &schema_type) {
	IcebergTableMetadata result {IcebergTableMetadataSchemas()};
	optional_ptr<const IcebergTableSchema> selected_schema;
	auto doc = JSONDocument::Parse(text.c_str(), text.size());
	auto metadata = rest_api_objects::TableMetadata::FromJSON(doc->GetRoot());
	if (!metadata.location || !metadata.last_updated_ms) {
		throw InvalidInputException("iceberg_scan_tasks metadata requires location and last-updated-ms");
	}
	if (metadata.schemas) {
		for (auto &item : *metadata.schemas) {
			if (!item.object_1.schema_id) {
				throw InvalidInputException("iceberg_scan_tasks metadata schema requires schema-id");
			}
		}
	}
	if (metadata.partition_specs) {
		for (auto &item : *metadata.partition_specs) {
			if (!item.spec_id) {
				throw InvalidInputException("iceberg_scan_tasks metadata partition spec requires spec-id");
			}
		}
	}
	result = IcebergTableMetadata::FromTableMetadata(metadata);
	result.GetSchemas().ForEachSchema([&](const IcebergTableSchema &candidate) {
		if (candidate.schema_id == schema_id) {
			selected_schema = candidate;
		}
	});
	if (!selected_schema) {
		throw InvalidInputException("iceberg_scan_tasks schema_id is absent from the metadata");
	}
	if (SchemaType(*selected_schema) != schema_type) {
		throw InvalidInputException("iceberg_scan_tasks schema column does not match the selected metadata schema");
	}
	if (!snapshot.IsNull() && !result.GetSnapshotById(BigIntValue::Get(snapshot))) {
		throw InvalidInputException("iceberg_scan_tasks snapshot_id is absent from the metadata");
	}
	return result;
}

Value IcebergScanTaskCodec::WriteDeleteFile(const IcebergDeleteFile &file) {
	vector<Value> equality_ids;
	for (auto id : file.equality_ids) {
		equality_ids.push_back(Value::INTEGER(id));
	}
	return Value::STRUCT(
	    DeleteFileType(),
	    {Value(file.file_path), Value(file.file_format), Value::INTEGER(static_cast<int32_t>(file.content)),
	     Value::BIGINT(file.file_size_in_bytes), Value::BIGINT(file.record_count),
	     Value::LIST(LogicalType::INTEGER, std::move(equality_ids)),
	     file.referenced_data_file ? Value(*file.referenced_data_file) : Value(LogicalType::VARCHAR),
	     file.content_offset ? Value::BIGINT(*file.content_offset) : Value(LogicalType::BIGINT),
	     file.content_size_in_bytes ? Value::BIGINT(*file.content_size_in_bytes) : Value(LogicalType::BIGINT)});
}

void IcebergScanTaskCodec::WriteTask(const IcebergFileScanTask &task, DataChunk &output, idx_t row) {
	output.data[FILE_PATH].SetValue(row, Value(task.file_path));
	output.data[FILE_FORMAT].SetValue(row, Value(task.file_format));
	output.data[FILE_SIZE].SetValue(row, Value::BIGINT(task.file_size_in_bytes));
	output.data[RECORD_COUNT].SetValue(row, Value::BIGINT(task.record_count));
	output.data[SEQUENCE_NUMBER].SetValue(row, task.sequence_number ? Value::BIGINT(*task.sequence_number)
	                                                                : Value(LogicalType::BIGINT));
	output.data[FIRST_ROW_ID].SetValue(row, task.first_row_id ? Value::BIGINT(*task.first_row_id)
	                                                          : Value(LogicalType::BIGINT));
	output.data[PARTITION_SPEC_ID].SetValue(row, Value::INTEGER(task.partition_spec_id));
	auto &partition_type = output.data[PARTITION_CONSTANTS].GetType();
	vector<Value> constants;
	for (auto &field : StructType::GetChildTypes(partition_type)) {
		auto id = IntegerValue::Get(Value(field.first).DefaultCastAs(LogicalType::INTEGER));
		auto value = task.partition_constants.find(id);
		constants.push_back(value == task.partition_constants.end() ? Value(field.second) : value->second);
	}
	output.data[PARTITION_CONSTANTS].SetValue(row, Value::STRUCT(partition_type, std::move(constants)));
	vector<Value> deletes;
	for (auto &file : task.delete_files) {
		deletes.push_back(WriteDeleteFile(file));
	}
	output.data[DELETE_FILES].SetValue(row, Value::LIST(DeleteFileType(), std::move(deletes)));
}

void IcebergScanTaskCodec::WriteContext(DataChunk &output, idx_t count, optional<int64_t> snapshot_id,
                                        int32_t schema_id, Vector &metadata) {
	output.data[SNAPSHOT_ID].Reference(snapshot_id ? Value::BIGINT(*snapshot_id) : Value(LogicalType::BIGINT),
	                                   count_t(count));
	output.data[SCHEMA_ID].Reference(Value::INTEGER(schema_id), count_t(count));
	output.data[METADATA].Reference(metadata);
	output.data[SCHEMA].Reference(Value(output.data[SCHEMA].GetType()), count_t(count));
	output.SetChildCardinality(count);
}

} // namespace duckdb
