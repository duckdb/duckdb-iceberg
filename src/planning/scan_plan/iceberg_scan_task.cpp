#include "planning/scan_plan/iceberg_scan_task.hpp"

namespace duckdb {

LogicalType IcebergScanTaskFormat::DeleteFileType() {
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

LogicalType IcebergScanTaskFormat::SchemaType(const IcebergTableSchema &schema) {
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

child_list_t<LogicalType> IcebergScanTaskFormat::Columns(const LogicalType &partition_type,
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
	        {"schema", schema_type}};
}

IcebergManifestEntry IcebergScanTaskFormat::ReadDeleteFile(const Value &descriptor) {
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
	IcebergManifestEntry result;
	result.status = IcebergManifestEntryStatusType::EXISTING;
	auto &file = result.data_file;
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
	return result;
}

OpenFileInfo IcebergScanTaskFormat::FileInfo(const string &path, const string &format, int64_t size,
                                             optional<int64_t> first_row_id, optional<int64_t> sequence_number) {
	if (!StringUtil::CIEquals(format, "parquet")) {
		throw NotImplementedException("File format '%s' not supported, only supports 'parquet' currently", format);
	}
	if (path.empty() || size < 0) {
		throw InvalidInputException("Iceberg data file requires a path and nonnegative size");
	}
	OpenFileInfo result(path);
	result.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	auto &options = result.extended_info->options;
	options["file_size"] = Value::UBIGINT(size);
	options["validate_external_file_cache"] = Value::BOOLEAN(false);
	options["etag"] = Value("");
	options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
	if (first_row_id) {
		options["first_row_id"] = Value::BIGINT(*first_row_id);
	}
	if (sequence_number) {
		options["sequence_number"] = Value::BIGINT(*sequence_number);
	}
	return result;
}

} // namespace duckdb
