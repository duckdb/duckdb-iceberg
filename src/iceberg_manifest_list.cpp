#include "metadata/iceberg_manifest_list.hpp"
#include "duckdb/main/database.hpp"

#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

namespace manifest_list {

static LogicalType FieldSummaryType() {
	child_list_t<LogicalType> children;
	children.emplace_back("contains_null", LogicalType::BOOLEAN);
	children.emplace_back("contains_nan", LogicalType::BOOLEAN);
	children.emplace_back("lower_bound", LogicalType::BLOB);
	children.emplace_back("upper_bound", LogicalType::BLOB);
	auto field_summary = LogicalType::STRUCT(children);

	return LogicalType::LIST(field_summary);
}

static Value FieldSummaryFieldIds() {
	child_list_t<Value> children;
	children.emplace_back("contains_null", Value::INTEGER(FIELD_SUMMARY_CONTAINS_NULL));
	children.emplace_back("contains_nan", Value::INTEGER(FIELD_SUMMARY_CONTAINS_NAN));
	children.emplace_back("lower_bound", Value::INTEGER(FIELD_SUMMARY_LOWER_BOUND));
	children.emplace_back("upper_bound", Value::INTEGER(FIELD_SUMMARY_UPPER_BOUND));
	children.emplace_back("__duckdb_field_id", Value::INTEGER(PARTITIONS_ELEMENT));
	auto field_summary = Value::STRUCT(children);

	child_list_t<Value> list_children;
	list_children.emplace_back("element", field_summary);
	list_children.emplace_back("__duckdb_field_id", Value::INTEGER(PARTITIONS));
	return Value::STRUCT(list_children);
}

void WriteToFile(const IcebergManifestList &manifest_list, CopyFunction &copy, DatabaseInstance &db,
                 ClientContext &context) {
	auto &allocator = db.GetBufferManager().GetBufferAllocator();

	//! Create the types for the DataChunk

	child_list_t<Value> field_ids;
	vector<string> names;
	vector<LogicalType> types;

	// manifest_path: string - 500
	names.push_back("manifest_path");
	types.push_back(LogicalType::VARCHAR);
	field_ids.emplace_back("manifest_path", Value::INTEGER(MANIFEST_PATH));

	// manifest_length: long - 501
	names.push_back("manifest_length");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("manifest_length", Value::INTEGER(MANIFEST_LENGTH));

	// partition_spec_id: long - 502
	names.push_back("partition_spec_id");
	types.push_back(LogicalType::INTEGER);
	field_ids.emplace_back("partition_spec_id", Value::INTEGER(PARTITION_SPEC_ID));

	// content: int - 517
	names.push_back("content");
	types.push_back(LogicalType::INTEGER);
	field_ids.emplace_back("content", Value::INTEGER(CONTENT));

	// sequence_number: long - 515
	names.push_back("sequence_number");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("sequence_number", Value::INTEGER(SEQUENCE_NUMBER));

	// min_sequence_number: long - 516
	names.push_back("min_sequence_number");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("min_sequence_number", Value::INTEGER(MIN_SEQUENCE_NUMBER));

	// added_snapshot_id: long - 503
	names.push_back("added_snapshot_id");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("added_snapshot_id", Value::INTEGER(ADDED_SNAPSHOT_ID));

	// added_files_count: int - 504
	names.push_back("added_files_count");
	types.push_back(LogicalType::INTEGER);
	field_ids.emplace_back("added_files_count", Value::INTEGER(ADDED_FILES_COUNT));

	// existing_files_count: int - 505
	names.push_back("existing_files_count");
	types.push_back(LogicalType::INTEGER);
	field_ids.emplace_back("existing_files_count", Value::INTEGER(EXISTING_FILES_COUNT));

	// deleted_files_count: int - 506
	names.push_back("deleted_files_count");
	types.push_back(LogicalType::INTEGER);
	field_ids.emplace_back("deleted_files_count", Value::INTEGER(DELETED_FILES_COUNT));

	// added_rows_count: long - 512
	names.push_back("added_rows_count");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("added_rows_count", Value::INTEGER(ADDED_ROWS_COUNT));

	// existing_rows_count: long - 513
	names.push_back("existing_rows_count");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("existing_rows_count", Value::INTEGER(EXISTING_ROWS_COUNT));

	// deleted_rows_count: long - 514
	names.push_back("deleted_rows_count");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("deleted_rows_count", Value::INTEGER(DELETED_ROWS_COUNT));

	// partitions: list<508: field_summary> - 507
	names.push_back("partitions");
	types.push_back(FieldSummaryType());
	field_ids.emplace_back("partitions", FieldSummaryFieldIds());

	//! Populate the DataChunk with the manifests

	DataChunk data;
	data.Initialize(allocator, types, manifest_list.manifests.size());

	for (idx_t i = 0; i < manifest_list.manifests.size(); i++) {
		auto &manifest = manifest_list.manifests[i];
		idx_t col_idx = 0;

		// manifest_path: string - 500
		data.SetValue(col_idx++, i, Value(manifest.manifest_path));

		// manifest_length: long - 501
		data.SetValue(col_idx++, i, Value::BIGINT(manifest.manifest_length));

		// partition_spec_id: long - 502
		data.SetValue(col_idx++, i, Value::BIGINT(manifest.partition_spec_id));

		// content: int - 517
		data.SetValue(col_idx++, i, Value::INTEGER(static_cast<int32_t>(manifest.content)));

		// sequence_number: long - 515
		data.SetValue(col_idx++, i, Value::BIGINT(manifest.sequence_number));

		// min_sequence_number: long - 516
		if (!manifest.has_min_sequence_number) {
			//! Behavior copied from pyiceberg
			data.SetValue(col_idx++, i, Value::BIGINT(-1));
		} else {
			data.SetValue(col_idx++, i, Value::BIGINT(manifest.min_sequence_number));
		}

		// added_snapshot_id: long - 503
		data.SetValue(col_idx++, i, Value::BIGINT(manifest.added_snapshot_id));

		// added_files_count: int - 504
		data.SetValue(col_idx++, i, Value::INTEGER(manifest.added_files_count));

		// existing_files_count: int - 505
		data.SetValue(col_idx++, i, Value::INTEGER(manifest.existing_files_count));

		// deleted_files_count: int - 506
		data.SetValue(col_idx++, i, Value::INTEGER(manifest.deleted_files_count));

		// added_rows_count: long - 512
		data.SetValue(col_idx++, i, Value::BIGINT(static_cast<int64_t>(manifest.added_rows_count)));

		// existing_rows_count: long - 513
		data.SetValue(col_idx++, i, Value::BIGINT(static_cast<int64_t>(manifest.existing_rows_count)));

		// deleted_rows_count: long - 514
		data.SetValue(col_idx++, i, Value::BIGINT(static_cast<int64_t>(manifest.deleted_rows_count)));

		// partitions: list<508: field_summary> - 507
		data.SetValue(col_idx++, i, manifest.partitions.ToValue());
	}
	data.SetCardinality(manifest_list.manifests.size());

	CopyInfo copy_info;
	copy_info.is_from = false;
	copy_info.options["root_name"].push_back(Value("manifest_file"));
	copy_info.options["field_ids"].push_back(Value::STRUCT(field_ids));

	CopyFunctionBindInput input(copy_info);
	input.file_extension = "avro";

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto bind_data = copy.copy_to_bind(context, input, names, types);

	auto global_state = copy.copy_to_initialize_global(context, *bind_data, manifest_list.path);
	auto local_state = copy.copy_to_initialize_local(execution_context, *bind_data);

	copy.copy_to_sink(execution_context, *bind_data, *global_state, *local_state, data);
	copy.copy_to_combine(execution_context, *bind_data, *global_state, *local_state);
	copy.copy_to_finalize(context, *bind_data, *global_state);
}

} // namespace manifest_list

} // namespace duckdb
