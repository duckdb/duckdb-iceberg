#include "execution/operator/iceberg_insert.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/planner/expression_binder/table_function_binder.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/string_util.hpp"

#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "execution/operator/iceberg_delete.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "core/metadata/schema/iceberg_column_definition.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "planning/iceberg_multi_file_list.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "core/expression/iceberg_value.hpp"
#include "core/expression/iceberg_metrics.hpp"
#include "core/expression/iceberg_transform.hpp"
#include "storage/statistics/iceberg_data_file_stats.hpp"
#include "catalog/rest/api/iceberg_type.hpp"
#include "catalog/rest/api/iceberg_create_table_request.hpp"
#include "common/iceberg_utils.hpp"
#include "catalog/rest/transaction/iceberg_transaction_update.hpp"
#include "iceberg_logging.hpp"
#include "iceberg_options.hpp"

namespace duckdb {

static bool WriteRowId(IcebergInsertVirtualColumns virtual_columns) {
	return virtual_columns == IcebergInsertVirtualColumns::WRITE_ROW_ID ||
	       virtual_columns == IcebergInsertVirtualColumns::WRITE_ROW_ID_AND_SEQUENCE_NUMBER;
}

static bool WriteSequenceNumber(IcebergInsertVirtualColumns virtual_columns) {
	return virtual_columns == IcebergInsertVirtualColumns::WRITE_SEQUENCE_NUMBER ||
	       virtual_columns == IcebergInsertVirtualColumns::WRITE_ROW_ID_AND_SEQUENCE_NUMBER;
}

IcebergInsert::IcebergInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
                             physical_index_vector_t<idx_t> column_index_map_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(&table),
      column_index_map(std::move(column_index_map_p)) {
}

IcebergInsert::IcebergInsert(PhysicalPlan &physical_plan, LogicalOperator &op)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(nullptr) {
}

IcebergInsert::IcebergInsert(PhysicalPlan &physical_plan, const vector<LogicalType> &types, TableCatalogEntry &table)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types, 1), table(&table) {
}

IcebergCopyOptions::IcebergCopyOptions(unique_ptr<CopyInfo> info_p, CopyFunction copy_function_p)
    : info(std::move(info_p)), copy_function(std::move(copy_function_p)) {
}

IcebergCopyInput::IcebergCopyInput(ClientContext &context, const IcebergTableMetadata &table_metadata,
                                   const IcebergTableSchema &schema, unique_ptr<BoundCreateTableInfo> ctas_info_p)
    : table_metadata(table_metadata), schema(schema), ctas_info(std::move(ctas_info_p)) {
	if (!ctas_info) {
		auto &fs = FileSystem::GetFileSystem(context);
		data_path = table_metadata.GetDataPath(fs);
	}

	// Get partition spec if the table is partitioned
	if (table_metadata.GetLatestPartitionSpec().IsPartitioned()) {
		partition_spec = table_metadata.FindPartitionSpecById(table_metadata.default_spec_id);
	}
}

static void StripTrailingSeparator(FileSystem &fs, string &path) {
	auto sep = fs.PathSeparator(path);
	if (!StringUtil::EndsWith(path, sep)) {
		return;
	}
	path = path.substr(0, path.size() - sep.size());
}

IcebergInsertGlobalState::IcebergInsertGlobalState(ClientContext &context)
    : GlobalSinkState(), context(context), insert_count(0) {
}

unique_ptr<GlobalSinkState> IcebergInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<IcebergInsertGlobalState>(context);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

static vector<idx_t> GetColumnPath(const ColumnIndex &column_index) {
	vector<idx_t> path;
	path.reserve(column_index.ChildIndexCount());
	for (auto &child_index : column_index.GetChildIndexes()) {
		path.push_back(child_index.GetPrimaryIndex());
	}
	return path;
}

static ColumnIndex GetColumnIndexBySourceId(const IcebergTableSchema &schema, idx_t source_id) {
	auto column_index = schema.TryGetColumnIndexByFieldId(source_id);
	if (!column_index) {
		throw InvalidInputException("Partition source column with id %d not found in schema", source_id);
	}
	return *column_index;
}

static bool IsTopLevelColumnSourceId(const IcebergTableSchema &schema, idx_t source_id) {
	auto column_index = GetColumnIndexBySourceId(schema, source_id);
	return column_index.ChildIndexCount() == 0;
}

static string GetColumnNameBySourceId(const IcebergTableSchema &schema, idx_t source_id) {
	return schema.GetColumnByFieldId(source_id).name;
}

//! SPEC (Appendix A): `unknown` has no physical Parquet type and must be omitted from data files.
static bool IsWrittenToDataFile(const LogicalType &type) {
	return type.id() != LogicalTypeId::SQLNULL;
}

//! The type used to write a column, with the omitted fields left out. Parquet keeps nullness in the definition
//! levels of a value's leaves, so a container left without any leaf cannot be written. `path` names the position
//! of `type` in the column, so that an error points at the container that has no leaf left.
static LogicalType GetWrittenType(const LogicalType &type, const string &path) {
	D_ASSERT(IsWrittenToDataFile(type));
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		child_list_t<LogicalType> children;
		for (auto &child : StructType::GetChildTypes(type)) {
			if (!IsWrittenToDataFile(child.second)) {
				continue;
			}
			children.emplace_back(child.first, GetWrittenType(child.second, path + "." + child.first));
		}
		if (children.empty()) {
			throw NotImplementedException(
			    "Cannot write \"%s\": a struct whose fields are all of type unknown has no data file representation",
			    path);
		}
		return LogicalType::STRUCT(std::move(children));
	}
	case LogicalTypeId::LIST: {
		auto &element = ListType::GetChildType(type);
		if (!IsWrittenToDataFile(element)) {
			throw NotImplementedException(
			    "Cannot write \"%s\": a list element of type unknown has no data file representation", path);
		}
		return LogicalType::LIST(GetWrittenType(element, path + ".element"));
	}
	case LogicalTypeId::MAP: {
		auto &key = MapType::KeyType(type);
		auto &value = MapType::ValueType(type);
		if (!IsWrittenToDataFile(key) || !IsWrittenToDataFile(value)) {
			throw NotImplementedException(
			    "Cannot write \"%s\": a map key or value of type unknown has no data file representation", path);
		}
		return LogicalType::MAP(GetWrittenType(key, path + ".key"), GetWrittenType(value, path + ".value"));
	}
	default:
		return type;
	}
}

//! FIELD_IDS entry for a written column: its field id, or the ids of the fields it is written with.
static Value GetWrittenFieldIdValue(const IcebergColumnDefinition &column) {
	auto column_value = Value::BIGINT(column.id);
	if (!column.GetChildCount()) {
		return column_value;
	}
	child_list_t<Value> values;
	values.emplace_back("__duckdb_field_id", std::move(column_value));
	for (auto &child : column.GetChildren()) {
		if (!IsWrittenToDataFile(child->type)) {
			continue;
		}
		values.emplace_back(child->name, GetWrittenFieldIdValue(*child));
	}
	return Value::STRUCT(std::move(values));
}

//! Whether every partition value is a top-level written column, so the copy operator can partition on the
//! columns themselves. Transforms, nested sources and columns absent from the data file (unknown) need a
//! computed partition value instead.
static bool CanWriteIdentityPartitionsDirectly(const IcebergPartitionSpec &spec, const IcebergTableSchema &schema) {
	for (auto &field : spec.fields) {
		if (field.transform.Type() == IcebergTransformType::VOID) {
			continue;
		}
		if (field.transform.Type() != IcebergTransformType::IDENTITY) {
			return false;
		}
		if (!IsTopLevelColumnSourceId(schema, field.source_id)) {
			return false;
		}
		if (!IsWrittenToDataFile(schema.GetColumnByFieldId(field.source_id).type)) {
			return false;
		}
	}
	return true;
}

void IcebergInsertGlobalState::AddFiles(DataChunk &chunk, const string &table_name,
                                        const IcebergTableMetadata &table_metadata) {
	// grab lock for written files vector
	lock_guard<mutex> guard(lock);
	for (idx_t r = 0; r < chunk.size(); r++) {
		IcebergManifestEntry manifest_entry;
		manifest_entry.status = IcebergManifestEntryStatusType::ADDED;

		// returned chunk has data as defined in
		// GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS)
		auto &data_file = manifest_entry.data_file;
		data_file.file_path = chunk.GetValue(0, r).GetValue<string>();
		data_file.record_count = static_cast<int64_t>(chunk.GetValue(1, r).GetValue<idx_t>());
		data_file.file_size_in_bytes = static_cast<int64_t>(chunk.GetValue(2, r).GetValue<idx_t>());
		data_file.content = IcebergManifestEntryContentType::DATA;
		data_file.file_format = "parquet";

		// extract the column stats
		auto column_stats = chunk.GetValue(4, r);

		// column 5 is stats, which we can also use for partition information
		auto partition_values = chunk.GetValue(5, r);

		auto table_current_schema_id = table_metadata.GetCurrentSchemaId();
		auto &ic_schema = table_metadata.GetSchemaFromId(table_current_schema_id);

		auto ic_partition_info = table_metadata.GetLatestPartitionSpec();

		// Build a map from partition column name to its partition spec field
		// To be used later to add partitioning info to the data file
		case_insensitive_map_t<reference<const IcebergPartitionSpecField>> partition_colname_to_field;

		// this is a weird case with partitioned inserts.
		// Lakekeeper requires paritition fields to not have the same names as the columns (if there is a transform)
		// So now our partition field names always include the transform name
		// But if there are only identity transforms, we don't add a projection to the insert, so we can just use
		// regular column names. So here when we populate our map, if there are transforms present, we need to use our
		// transform partition column names. If not, we should use the identify names.
		if (!CanWriteIdentityPartitionsDirectly(ic_partition_info, ic_schema)) {
			for (auto &partition_field : ic_partition_info.fields) {
				partition_colname_to_field.emplace(partition_field.GetPartitionSpecFieldName(), partition_field);
			}
		} else {
			for (auto &partition_field : ic_partition_info.fields) {
				auto actual_col_name = GetColumnNameBySourceId(ic_schema, partition_field.source_id);
				partition_colname_to_field.emplace(actual_col_name, partition_field);
			}
		}

		if (!partition_values.IsNull()) {
			// Populate partition_info from the partition values in the chunk
			auto &partition_children = MapValue::GetChildren(partition_values);
			for (auto &partition_val : partition_children) {
				auto &struct_val = StructValue::GetChildren(partition_val);
				auto &partition_name = StringValue::Get(struct_val[0]);

				auto field_it = partition_colname_to_field.find(partition_name);
				D_ASSERT(field_it != partition_colname_to_field.end());
				auto &partition_field = field_it->second.get();
				auto source_type = ic_schema.GetColumnTypeFromFieldId(partition_field.source_id);

				IcebergPartitionInfo info;
				info.field_id = partition_field.partition_field_id;
				if (!struct_val[1].IsNull()) {
					info.value = Value(StringValue::Get(struct_val[1]));
				} else {
					info.value = Value();
				}
				data_file.partition_info.push_back(std::move(info));
			}
		}
		// Partitioning alone does not imply sorted data. Only advertise the explicit sort order
		// enforced by PlanCopyForInsert (PhysicalOrder or the partitioned copy writer).
		if (table_metadata.HasSortOrder()) {
			auto &sort_order = table_metadata.GetLatestSortOrder();
			if (sort_order.IsSorted()) {
				data_file.sort_order_id = sort_order.sort_order_id;
			}
		}

		insert_count += data_file.record_count;

		IcebergDataFileStats::PopulateFromReturnStats(context, data_file, column_stats, table_metadata, table_name);
		DUCKDB_LOG(context, IcebergLogType,
		           "Iceberg INSERT, wrote data_file '%s', record_count=%lld, file_size=%lld bytes", data_file.file_path,
		           data_file.record_count, data_file.file_size_in_bytes);

		written_files.push_back(std::move(manifest_entry));
	}
}

void IcebergInsert::AddWrittenFiles(IcebergInsertGlobalState &global_state, DataChunk &chunk,
                                    optional_ptr<TableCatalogEntry> table) {
	D_ASSERT(table);
	auto &ic_table = table->Cast<IcebergTableSchemaVersion>();
	auto &table_metadata = ic_table.table_info.table_metadata;
	global_state.AddFiles(chunk, ic_table.name.GetIdentifierName(), table_metadata);
}

optional_ptr<TableCatalogEntry> IcebergInsert::GetEffectiveTable() const {
	if (table) {
		return table;
	}
	if (ctas_copy_op) {
		auto created_table = ctas_copy_op->GetCreatedTable();
		return created_table ? created_table.get() : nullptr;
	}
	return nullptr;
}

SinkResultType IcebergInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergInsertGlobalState>();

	// For CTAS, `table` is null at planning time and the catalog entry is
	// produced by the IcebergCopyToFile below this insert. GetGlobalSinkState in
	// IcebergCopyToFile will create the table, so we can resolve the effective
	// table from there
	auto effective_table = GetEffectiveTable();
	AddWrittenFiles(global_state, chunk, effective_table);

	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType IcebergInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &global_state = sink_state->Cast<IcebergInsertGlobalState>();
	auto value = Value::BIGINT(global_state.insert_count);
	chunk.data[0].Append(value);
	chunk.SetChildCardinality(1);
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType IcebergInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergInsertGlobalState>();

	auto effective_table = GetEffectiveTable();
	if (!effective_table) {
		// Table does not exist (INSERT INTO) or was not created in Physical Create Iceberg table (CTAS). Throw Error
		throw InternalException("Table to insert into does not exist.");
	}
	auto &irc_table = effective_table->Cast<IcebergTableSchemaVersion>();
	auto &table_info = irc_table.table_info;
	auto &transaction = IcebergTransaction::Get(context, effective_table->catalog);
	auto &iceberg_transaction = transaction.Cast<IcebergTransaction>();

	vector<IcebergManifestEntry> written_files;
	{
		lock_guard<mutex> guard(global_state.lock);
		written_files = std::move(global_state.written_files);
	}

	if (update_delete_op) {
		// This insert is part of an UPDATE: commit a combined delete+insert snapshot.
		auto &delete_global_state = update_delete_op->sink_state->Cast<IcebergDeleteGlobalState>();
		auto delete_manifest_entries = IcebergDelete::GenerateDeleteManifestEntries(delete_global_state);
		if (!written_files.empty()) {
			ApplyTableUpdate(table_info, iceberg_transaction, [&](IcebergTable &tbl) {
				auto &transaction_data = tbl.GetOrCreateTransactionData(iceberg_transaction);
				transaction_data.AddUpdateSnapshot(std::move(delete_manifest_entries), std::move(written_files),
				                                   std::move(delete_global_state.altered_manifests));
			});
		}
	} else {
		// Regular insert: commit an append snapshot.
		if (!written_files.empty()) {
			ApplyTableUpdate(table_info, iceberg_transaction, [&](IcebergTable &tbl) {
				auto &transaction_data = tbl.GetOrCreateTransactionData(iceberg_transaction);
				IcebergManifestDeletes empty_deletes;
				transaction_data.AddSnapshot(IcebergSnapshotOperationType::APPEND, std::move(written_files),
				                             std::move(empty_deletes));
			});
		}
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string IcebergInsert::GetName() const {
	return "ICEBERG_INSERT";
}

InsertionOrderPreservingMap<string> IcebergInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	if (table) {
		result["Table Name"] = table->name.GetIdentifierName();
	} else if (ctas_copy_op) {
		auto created_table = ctas_copy_op->GetCreatedTable();
		if (created_table) {
			result["Table Name"] = created_table->name.GetIdentifierName();
		}
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Partition Expression Generation
//===--------------------------------------------------------------------===//

static unique_ptr<Expression> CreateSourceColumnReference(ClientContext &context, const IcebergCopyInput &copy_input,
                                                          uint64_t source_id) {
	auto column_index = GetColumnIndexBySourceId(copy_input.schema, source_id);
	auto primary_index = column_index.GetPrimaryIndex();
	auto &root_column = *copy_input.schema.columns[primary_index];
	unique_ptr<Expression> result = make_uniq<BoundReferenceExpression>(root_column.type, primary_index);
	for (auto &child_index : GetColumnPath(column_index)) {
		vector<unique_ptr<Expression>> children;
		children.push_back(std::move(result));
		children.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(NumericCast<int64_t>(child_index + 1))));

		ErrorData error;
		FunctionBinder binder(context);
		result = binder.BindScalarFunction(Identifier::DefaultSchema(), Identifier("struct_extract_at"),
		                                   std::move(children), error, false);
		if (!result) {
			error.Throw();
		}
	}
	return result;
}

static unique_ptr<Expression> BindTransformFunction(ClientContext &context, const string &name,
                                                    vector<unique_ptr<Expression>> children) {
	ErrorData error;
	FunctionBinder binder(context);
	auto function =
	    binder.BindScalarFunction(Identifier::DefaultSchema(), Identifier(name), std::move(children), error, false);
	if (!function) {
		error.Throw();
	}
	return function;
}

//! Iceberg partition/sort transforms for year/month/day/hour are defined as:
//! - years: date_diff('year', DATE '1970-01-01', source_column)
//! - months: date_diff('month', DATE '1970-01-01', source_column)
//! - days: date_diff('day', DATE '1970-01-01', source_column)
//! - hours: date_diff('hour', TIMESTAMP '1970-01-01', source_column)
static unique_ptr<Expression> GetDateDiffFunction(ClientContext &context, const IcebergCopyInput &copy_input,
                                                  const string &date_part, uint64_t source_id) {
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundConstantExpression>(Value(date_part)));
	if (date_part == "hour") {
		children.push_back(make_uniq<BoundConstantExpression>(Value::TIMESTAMP(Timestamp::FromEpochSeconds(0))));
	} else {
		children.push_back(make_uniq<BoundConstantExpression>(Value::DATE(Date::FromDate(Date::EPOCH_YEAR, 1, 1))));
	}

	auto source = CreateSourceColumnReference(context, copy_input, source_id);
	auto source_type = source->GetReturnType().id();
	auto target_type = source->GetReturnType();
	if (source_type == LogicalTypeId::TIMESTAMP_TZ) {
		target_type = LogicalType::TIMESTAMP;
	}
	if (source_type == LogicalTypeId::TIMESTAMP_TZ_NS) {
		target_type = LogicalType::TIMESTAMP_NS;
	}
	source = BoundCastExpression::AddDefaultCastToType(std::move(source), target_type);
	children.push_back(std::move(source));
	return BindTransformFunction(context, "date_diff", std::move(children));
}

static unique_ptr<Expression> GetBucketExpression(ClientContext &context, const IcebergCopyInput &copy_input,
                                                  uint64_t source_id, const IcebergTransform &transform) {
	vector<unique_ptr<Expression>> children;
	children.push_back(
	    make_uniq<BoundConstantExpression>(Value::INTEGER(static_cast<int32_t>(transform.GetBucketModulo()))));
	children.push_back(CreateSourceColumnReference(context, copy_input, source_id));
	return BindTransformFunction(context, "iceberg_bucket", std::move(children));
}

static unique_ptr<Expression> GetTruncateExpression(ClientContext &context, const IcebergCopyInput &copy_input,
                                                    uint64_t source_id, const IcebergTransform &transform) {
	vector<unique_ptr<Expression>> children;
	children.push_back(
	    make_uniq<BoundConstantExpression>(Value::INTEGER(static_cast<int32_t>(transform.GetTruncateWidth()))));
	children.push_back(CreateSourceColumnReference(context, copy_input, source_id));
	return BindTransformFunction(context, "iceberg_truncate", std::move(children));
}

static unique_ptr<Expression> GetTransformExpression(ClientContext &context, const IcebergCopyInput &copy_input,
                                                     uint64_t source_id, const IcebergTransform &transform,
                                                     const char *usage) {
	switch (transform.Type()) {
	case IcebergTransformType::IDENTITY: {
		return CreateSourceColumnReference(context, copy_input, source_id);
	}
	case IcebergTransformType::YEAR:
		return GetDateDiffFunction(context, copy_input, "year", source_id);
	case IcebergTransformType::MONTH:
		return GetDateDiffFunction(context, copy_input, "month", source_id);
	case IcebergTransformType::DAY:
		return GetDateDiffFunction(context, copy_input, "day", source_id);
	case IcebergTransformType::HOUR:
		return GetDateDiffFunction(context, copy_input, "hour", source_id);
	case IcebergTransformType::BUCKET:
		return GetBucketExpression(context, copy_input, source_id, transform);
	case IcebergTransformType::TRUNCATE:
		return GetTruncateExpression(context, copy_input, source_id, transform);
	case IcebergTransformType::VOID:
		throw InvalidInputException("VOID partition transform should not be used for %s", usage);
	default:
		throw NotImplementedException("Unsupported %s transform type", usage);
	}
}

static OrderType GetDuckDBOrderType(const string &direction) {
	if (StringUtil::CIEquals(direction, "asc")) {
		return OrderType::ASCENDING;
	}
	if (StringUtil::CIEquals(direction, "desc")) {
		return OrderType::DESCENDING;
	}
	throw NotImplementedException("Unsupported Iceberg sort direction '%s'", direction);
}

static OrderByNullType GetDuckDBNullOrder(const string &null_order) {
	if (StringUtil::CIEquals(null_order, "nulls-first")) {
		return OrderByNullType::NULLS_FIRST;
	}
	if (StringUtil::CIEquals(null_order, "nulls-last")) {
		return OrderByNullType::NULLS_LAST;
	}
	throw NotImplementedException("Unsupported Iceberg null order '%s'", null_order);
}

static void GenerateSortOrderExpressions(ClientContext &context, const IcebergCopyInput &copy_input,
                                         IcebergCopyOptions &result) {
	if (!copy_input.table_metadata.HasSortOrder()) {
		return;
	}
	auto &sort_order = copy_input.table_metadata.GetLatestSortOrder();
	if (!sort_order.IsSorted()) {
		return;
	}
	for (auto &field : sort_order.fields) {
		auto expr = GetTransformExpression(context, copy_input, field.source_id, field.transform, "sorting");
		result.order_columns.emplace_back(GetDuckDBOrderType(field.direction), GetDuckDBNullOrder(field.null_order),
		                                  std::move(expr));
	}
}

//===--------------------------------------------------------------------===//
// Data file write layout
//===--------------------------------------------------------------------===//

namespace {

//! One column of the chunk handed to the copy operator, and how to produce it from the child plan.
struct IcebergWriteColumn {
	string name;
	LogicalType type;
	//! Expression over the child plan output (a plain column reference, or a partition transform). Used as the
	//! projection expression when one is needed, and to tell where the column comes from when it is not.
	unique_ptr<Expression> source;
	//! FIELD_IDS entry for the parquet writer; unset for columns that are not written to the file.
	optional<Value> field_id;
	//! Added only to route rows by a transformed partition value (e.g. day(ts)); not written to the file.
	bool is_computed_partition_value = false;
};

//! The layout of the chunk that reaches the copy operator. Built once, so that the copy bind
//! (names/types/FIELD_IDS), the projection and the partition column positions cannot disagree.
struct IcebergWriteLayout {
	vector<IcebergWriteColumn> columns;
	//! Positions in `columns` used to partition the output.
	vector<idx_t> partition_columns;

	//! Identity partitioning routes by written columns, which stay in the file; transformed partition values
	//! are computed columns that are stripped. PhysicalCopyToFile has a single flag, so they never mix.
	bool WritePartitionColumns() const {
		return partition_columns.empty() || !columns[partition_columns[0]].is_computed_partition_value;
	}
};

} // namespace

//! The child plan column a layout column passes through unchanged, if it is a plain reference.
static optional_idx PassThroughIndex(const IcebergWriteColumn &column) {
	if (column.source->GetExpressionType() != ExpressionType::BOUND_REF) {
		return optional_idx();
	}
	return column.source->Cast<BoundReferenceExpression>().Index();
}

//! Position in the layout of the written column that passes through the given child plan column.
static idx_t FindWrittenColumn(const IcebergWriteLayout &layout, idx_t child_idx) {
	for (idx_t i = 0; i < layout.columns.size(); i++) {
		auto &column = layout.columns[i];
		if (!column.is_computed_partition_value && PassThroughIndex(column) == child_idx) {
			return i;
		}
	}
	throw InternalException("Child plan column %d is not written to the data file", child_idx);
}

//! Number of columns the child plan produces: the schema columns followed by the virtual columns.
static idx_t ChildColumnCount(const IcebergCopyInput &copy_input) {
	idx_t count = copy_input.schema.columns.size();
	if (WriteRowId(copy_input.virtual_columns)) {
		count++;
	}
	if (WriteSequenceNumber(copy_input.virtual_columns)) {
		count++;
	}
	return count;
}

//! A projection is needed unless every column of the child plan output passes through unchanged.
static bool NeedsProjection(const IcebergWriteLayout &layout, const IcebergCopyInput &copy_input) {
	if (layout.columns.size() != ChildColumnCount(copy_input)) {
		return true;
	}
	for (idx_t i = 0; i < layout.columns.size(); i++) {
		if (PassThroughIndex(layout.columns[i]) != i) {
			return true;
		}
	}
	return false;
}

static IcebergWriteLayout BuildWriteLayout(ClientContext &context, const IcebergCopyInput &copy_input) {
	IcebergWriteLayout layout;
	auto &schema = copy_input.schema;

	// Physical columns, in schema order. The child plan produces every schema column, so the source
	// index is the schema index even when earlier columns are omitted.
	for (idx_t schema_idx = 0; schema_idx < schema.columns.size(); schema_idx++) {
		auto &column = *schema.columns[schema_idx];
		if (!IsWrittenToDataFile(column.type)) {
			// e.g. unknown: the column stays in the schema but has no data file representation
			continue;
		}
		IcebergWriteColumn write_column;
		write_column.name = column.name;
		write_column.type = GetWrittenType(column.type, column.name);
		write_column.source = make_uniq<BoundReferenceExpression>(column.type, schema_idx);
		if (write_column.type != column.type) {
			// casting to the written type drops the omitted fields from the value
			write_column.source =
			    BoundCastExpression::AddCastToType(context, std::move(write_column.source), write_column.type);
		}
		write_column.field_id = GetWrittenFieldIdValue(column);
		layout.columns.push_back(std::move(write_column));
	}

	// Virtual columns follow the schema columns in the child plan output.
	idx_t child_idx = schema.columns.size();
	if (WriteRowId(copy_input.virtual_columns)) {
		IcebergWriteColumn write_column;
		write_column.name = "_row_id";
		write_column.type = LogicalType::BIGINT;
		write_column.source = make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, child_idx++);
		write_column.field_id = Value::BIGINT(MultiFileReader::ROW_ID_FIELD_ID);
		layout.columns.push_back(std::move(write_column));
	}
	if (WriteSequenceNumber(copy_input.virtual_columns)) {
		IcebergWriteColumn write_column;
		write_column.name = "_last_updated_sequence_number";
		write_column.type = LogicalType::BIGINT;
		write_column.source = make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, child_idx++);
		write_column.field_id = Value::BIGINT(MultiFileReader::LAST_UPDATED_SEQUENCE_NUMBER_ID);
		layout.columns.push_back(std::move(write_column));
	}
	D_ASSERT(child_idx == ChildColumnCount(copy_input));

	if (!copy_input.partition_spec) {
		return layout;
	}
	auto &spec = *copy_input.partition_spec;

	if (CanWriteIdentityPartitionsDirectly(spec, schema)) {
		// All transforms are identity: partition on the written columns themselves.
		for (auto &field : spec.fields) {
			if (field.transform.Type() == IcebergTransformType::VOID) {
				continue;
			}
			auto schema_idx = GetColumnIndexBySourceId(schema, field.source_id).GetPrimaryIndex();
			layout.partition_columns.push_back(FindWrittenColumn(layout, schema_idx));
		}
		return layout;
	}

	// Otherwise every field, identity included, becomes a computed column at the end of the chunk: the copy
	// operator routes rows by it and strips it before writing.
	for (auto &field : spec.fields) {
		if (field.transform.Type() == IcebergTransformType::VOID) {
			continue;
		}
		IcebergWriteColumn write_column;
		write_column.name = field.GetPartitionSpecFieldName();
		write_column.source =
		    GetTransformExpression(context, copy_input, field.source_id, field.transform, "partitioning");
		write_column.type = write_column.source->GetReturnType();
		write_column.is_computed_partition_value = true;
		layout.partition_columns.push_back(layout.columns.size());
		layout.columns.push_back(std::move(write_column));
	}
	return layout;
}

vector<IcebergManifestEntry> IcebergInsert::GetInsertManifestEntries(IcebergInsertGlobalState &global_state) {
	lock_guard<mutex> guard(global_state.lock);
	return std::move(global_state.written_files);
}

namespace {

struct IcebergParquetOptionMapping {
	const char *iceberg_option;
	const char *parquet_option;
};

// Maps from
// https://iceberg.apache.org/docs/1.10.0/configuration/#write-properties
// to
// https://github.com/duckdb/duckdb/blob/9cbb0656cd34fa3eb890963b9f961bbc8a221fa9/extension/parquet/parquet_extension.cpp#L121
static const IcebergParquetOptionMapping ICEBERG_TABLE_PROPERTY_MAPPING[] = {
    {"write.parquet.compression-codec", "codec"},
    {"write.parquet.compression-level", "compression_level"},
    {"write.parquet.dict-size-bytes", "string_dictionary_page_size_limit"},
    {"write.parquet.page-size-bytes", "data_page_size_limit"},
    {"write.parquet.row-group-size-bytes", "row_group_size_bytes"},
    {"write.parquet.row-group-size", "row_group_size"},
    {"write.parquet.row-groups-per-file", "row_groups_per_file"}};

static const idx_t ICEBERG_TABLE_PROPERTY_MAPPING_SIZE =
    sizeof(ICEBERG_TABLE_PROPERTY_MAPPING) / sizeof(IcebergParquetOptionMapping);

} // namespace

IcebergCopyOptions IcebergInsert::GetCopyOptions(ClientContext &context, const IcebergCopyInput &copy_input) {
	auto info = make_uniq<CopyInfo>();
	info->file_path = copy_input.data_path;

	auto file_format = "parquet";
	info->format = file_format;
	info->is_from = false;

	// Everything the copy operator is told about its input (bind names/types, FIELD_IDS, projection,
	// partition column positions) is derived from this single layout so that they cannot disagree.
	auto layout = BuildWriteLayout(context, copy_input);

	child_list_t<Value> field_id_values;
	for (auto &column : layout.columns) {
		if (column.field_id) {
			field_id_values.emplace_back(column.name, *column.field_id);
		}
	}
	vector<Value> field_input;
	field_input.push_back(Value::STRUCT(std::move(field_id_values)));
	info->options["field_ids"] = std::move(field_input);
	for (auto &option : copy_input.options) {
		info->options[option.first] = option.second;
	}

	const auto &table_properties = copy_input.table_metadata.GetTableProperties();
	// Map Iceberg write properties to DuckDB parquet copy options
	optional_idx batch_size;
	optional_idx batch_size_bytes;
	// TODO: Iceberg properties for bloom filter are per column, duckdb's seems to be per table.
	// write.parquet.bloom-filter-fpp.column.<col> -> bloom_filter_false_positive_ratio
	// write.parquet.bloom-filter-enabled.column.<col> -> write_bloom_filter
	for (idx_t i = 0; i < ICEBERG_TABLE_PROPERTY_MAPPING_SIZE; i++) {
		auto &mapping = ICEBERG_TABLE_PROPERTY_MAPPING[i];
		auto it = table_properties.find(mapping.iceberg_option);
		if (it == table_properties.end()) {
			continue;
		}
		// DuckDB's parquet copy_to_bind ignores row_group_size(_bytes); the COPY binder resolves them into
		// batch_size(_bytes) on the copy operator instead. Do the same here, since we bypass that binder.
		if (StringUtil::CIEquals(mapping.parquet_option, "row_group_size_bytes")) {
			batch_size_bytes = IcebergUtils::ParseByteSizeOptionallyFormatted(it->second);
			continue;
		}
		if (StringUtil::CIEquals(mapping.parquet_option, "row_group_size")) {
			batch_size = StringUtil::ToUnsigned(it->second);
			continue;
		}
		info->options[mapping.parquet_option].emplace_back(it->second);
	}

	// Always use native parquet geometry for writing
	info->options["geoparquet_version"].emplace_back("NONE");

	auto &fs = FileSystem::GetFileSystem(context);
	// data_path is empty while a CTAS is being planned, because its table does not have a location yet
	if (!copy_input.data_path.empty() && !fs.IsRemoteFile(copy_input.data_path)) {
		// create data path if it does not yet exist
		try {
			fs.CreateDirectoriesRecursive(copy_input.data_path);
		} catch (...) {
		}
	}

	// Bind Copy Function
	CopyFunctionBindInput bind_input(*info);

	// copy_to_bind receives only the columns that end up in the file. Computed partition values are
	// stripped by PhysicalCopyToFile before writing, so including them would cause a type mismatch.
	vector<string> names_to_write;
	vector<LogicalType> types_to_write;
	for (auto &column : layout.columns) {
		if (column.is_computed_partition_value) {
			continue;
		}
		names_to_write.push_back(column.name);
		types_to_write.push_back(column.type);
	}

	if (names_to_write.empty()) {
		throw NotImplementedException("Cannot write to a table where every column is of type unknown: unknown "
		                              "columns are omitted from data files, leaving no columns to write");
	}

	// Get Parquet Copy function
	auto &copy_fun = IcebergUtils::GetCopyFunction(context, Identifier(file_format));
	IcebergCopyOptions result(std::move(info), copy_fun.function);
	GenerateSortOrderExpressions(context, copy_input, result);

	result.filename_pattern.SetFilenamePattern("{uuidv7}");
	auto write_target_file_size = table_properties.find("write.target-file-size-bytes");
	if (write_target_file_size != table_properties.end()) {
		result.file_size_bytes = IcebergUtils::ParseByteSizeOptionallyFormatted(write_target_file_size->second);
	}
	if (copy_input.partition_spec) {
		result.partition_output = true;
		result.write_empty_file = true;
	} else {
		result.partition_output = false;
		result.write_empty_file = false;
	}

	result.file_path = copy_input.data_path;
	// A filesystem can throw if it has been disabled, so skip if the path is empty (namely for CTAS)
	if (!result.file_path.empty()) {
		StripTrailingSeparator(fs, result.file_path);
	}
	result.file_extension = file_format;
	result.overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE;
	result.per_thread_output = false;
	result.write_partition_columns = layout.WritePartitionColumns();
	result.partition_columns = std::move(layout.partition_columns);
	result.return_type = CopyFunctionReturnType::WRITTEN_FILE_STATISTICS;

	auto partitioned_paths = table_properties.find("write.object-storage.partitioned-paths");
	if (partitioned_paths != table_properties.end()) {
		result.partitioned_paths =
		    Value(partitioned_paths->second).DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
	}

	auto function_data =
	    copy_fun.function.copy_to_bind(context, bind_input, StringsToIdentifiers(names_to_write), types_to_write);
	result.bind_data = std::move(function_data);

	// Mirrors Binder::BindCopyTo: without any batch size the copy operator flushes every chunk, producing
	// one row group per vector (2048 rows). Table properties override the Iceberg defaults.
	if (batch_size.IsValid()) {
		result.batch_size = batch_size;
	}
	if (batch_size_bytes.IsValid()) {
		result.batch_size_bytes = batch_size_bytes;
	}
	if (!result.batch_size.IsValid() && !result.batch_size_bytes.IsValid() && copy_fun.function.desired_batch_size) {
		result.batch_size = copy_fun.function.desired_batch_size(context, *result.bind_data);
	}

	// A file can never be smaller than a single row group, and rotation to honor
	// write.target-file-size-bytes only happens at row-group boundaries. If the row-group size exceeds the
	// target file size, rotation can never split the output, so cap it to the target file size.
	if (result.batch_size_bytes.IsValid() && result.batch_size_bytes.GetIndex() > result.file_size_bytes) {
		result.batch_size_bytes = result.file_size_bytes;
	}

	// The chunk that reaches the copy operator: written columns followed by any computed partition values.
	const bool needs_projection = NeedsProjection(layout, copy_input);
	for (auto &column : layout.columns) {
		result.names.emplace_back(column.name);
		result.expected_types.push_back(column.type);
		if (needs_projection) {
			result.projection_list.push_back(std::move(column.source));
		}
	}

	return result;
}

static void GenerateProjection(ClientContext &context, PhysicalPlanGenerator &planner,
                               vector<unique_ptr<Expression>> &expressions, optional_ptr<PhysicalOperator> &plan) {
	// push the projection
	vector<LogicalType> types;
	for (auto &expr : expressions) {
		auto &type = expr->GetReturnType();
		if (type.id() == LogicalTypeId::HUGEINT) {
			expr->SetReturnType(LogicalType::DECIMAL(38, 0));
			types.push_back(expr->GetReturnType());
		} else {
			types.push_back(type);
		}
	}
	auto &proj =
	    planner.Make<PhysicalProjection>(std::move(types), std::move(expressions), plan->estimated_cardinality);
	proj.children.push_back(*plan);
	plan = proj;
}

static void GeneratePhysicalOrder(PhysicalPlanGenerator &planner, vector<BoundOrderByNode> &orders,
                                  optional_ptr<PhysicalOperator> &plan) {
	D_ASSERT(plan);
	vector<idx_t> projections;
	projections.reserve(plan->GetTypes().size());
	for (idx_t i = 0; i < plan->GetTypes().size(); i++) {
		projections.push_back(i);
	}
	auto &order = planner.Make<PhysicalOrder>(plan->GetTypes(), std::move(orders), std::move(projections),
	                                          plan->estimated_cardinality);
	order.children.push_back(*plan);
	plan = order;
}

IcebergCopyToFile &IcebergInsert::PlanCopyForInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                    IcebergCopyInput &copy_input, optional_ptr<PhysicalOperator> plan) {
	auto copy_options = GetCopyOptions(context, copy_input);
	D_ASSERT(!plan || plan->GetTypes().size() == ChildColumnCount(copy_input));

	// Sort expressions reference the child plan output, so order before the projection changes the layout.
	// Partitioned writes are sorted per partition by the copy operator itself.
	if (!copy_input.partition_spec && !copy_options.order_columns.empty() && plan) {
		GeneratePhysicalOrder(planner, copy_options.order_columns, plan);
	}

	// Produce the data file layout: drops columns that are not written (unknown) and computes
	// non-identity partition transforms.
	if (!copy_options.projection_list.empty() && plan) {
		GenerateProjection(context, planner, copy_options.projection_list, plan);
	}

	auto copy_return_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	// For CTAS the table does not exist yet, so the options resolved above are based on placeholder metadata
	// without a location. The copy creates the table and re-resolves them before it needs a path.
	auto &physical_copy = planner
	                          .Make<IcebergCopyToFile>(copy_return_types, std::move(copy_options.copy_function),
	                                                   nullptr, 1, std::move(copy_input.ctas_info))
	                          .Cast<IcebergCopyToFile>();

	physical_copy.ApplyCopyOptions(copy_options);
	if (plan) {
		physical_copy.children.push_back(*plan);
	}

	return physical_copy;
}

PhysicalOperator &IcebergInsert::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                            IcebergTableSchemaVersion &table) {
	optional_idx partition_id;
	vector<LogicalType> return_types;
	// the one return value is how many rows we are inserting
	return_types.emplace_back(LogicalType::BIGINT);
	return planner.Make<IcebergInsert>(return_types, table);
}

PhysicalOperator &IcebergCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                             optional_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for insertion into Iceberg table");
	}

	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT clause not yet supported for insertion into Iceberg table");
	}

	if (!op.column_index_map.empty()) {
		plan = planner.ResolveDefaultsProjection(op, *plan);
	}
	auto &table_entry = op.table.Cast<IcebergTableSchemaVersion>();
	table_entry.PrepareIcebergScanFromEntry(context);

	auto &irc_transaction = IcebergTransaction::Get(context, *this);

	auto &alter = irc_transaction.GetOrCreateAlter();
	auto &updated_table = alter.GetOrInitializeTable(table_entry.table_info);
	auto &table_metadata = updated_table.table_metadata;
	auto &schema = table_metadata.GetLatestSchema();
	auto &updated_table_entry = *updated_table.schema_versions[schema.schema_id];

	// Create Copy Info
	IcebergCopyInput copy_input(context, table_metadata, schema);
	auto &insert = planner.Make<IcebergInsert>(op, updated_table_entry, op.column_index_map);
	auto &physical_copy = IcebergInsert::PlanCopyForInsert(context, planner, copy_input, plan);
	insert.children.push_back(physical_copy);

	return insert;
}

static unique_ptr<IcebergTableMetadata> BuildPlaceholderMetadata(ClientContext &context, BoundCreateTableInfo &info) {
	auto metadata = make_uniq<IcebergTableMetadata>(IcebergTableMetadataSchemas {});
	metadata->iceberg_version = DEFAULT_ICEBERG_FORMAT_VERSION;
	metadata->default_spec_id = 0;
	metadata->SetCurrentSchemaId(0);

	Value default_version;
	if (context.TryGetCurrentSetting(DEFAULT_FORMAT_VERSION_CONFIG_VARIABLE, default_version)) {
		metadata->iceberg_version = default_version.GetValue<uint64_t>();
	}
	auto &create_info = info.Base().Cast<CreateTableInfo>();
	auto binder = Binder::CreateBinder(context);
	TableFunctionBinder property_binder(*binder, context, "format-version");
	for (auto &option : create_info.options) {
		auto expr_copy = option.second->Copy();
		auto bound_expr = property_binder.Bind(expr_copy);
		if (bound_expr->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		auto val = ExpressionExecutor::EvaluateScalar(context, *bound_expr, true);
		if (option.first == "format-version") {
			auto version = val.DefaultCastAs(LogicalType::INTEGER).GetValue<int32_t>();
			if (version < 1) {
				throw InvalidInputException("The lowest supported iceberg version is 1!");
			}
			metadata->iceberg_version = version;
		}
		metadata->table_properties[option.first] = val.GetValue<string>();
	}

	// Resolve nested field IDs and storage types exactly as table creation does.
	int32_t last_column_id;
	auto schema = IcebergCreateTableRequest::CreateIcebergSchema(context, *metadata, create_info.columns,
	                                                             &create_info.constraints, last_column_id);
	metadata->last_column_id = last_column_id;
	metadata->GetSchemasMutable().AddSchemaOrGetExisting(schema);

	// Build a placeholder partition spec from the parsed PARTITIONED BY clause so that
	// PlanCopyForInsert appends the partition projection at plan time. The real spec is
	// applied when IcebergCopyToFile creates the table, but the projection
	// indices are derived from the same partition_keys/schema and so remain consistent.
	auto placeholder_spec = IcebergTable::BuildPartitionSpec(create_info.partition_keys, *schema, 0, 1000);
	metadata->partition_specs.emplace(0, std::move(placeholder_spec));

	// Same for the SORTED BY clause: the ordering has to be part of the plan, so the placeholder metadata
	// carries the sort order that the created table will end up with.
	auto placeholder_sort_order_id = create_info.sort_keys.empty() ? UNSORTED_SORT_ORDER_ID : INITIAL_SORT_ORDER_ID;
	auto placeholder_sort_order =
	    IcebergTable::BuildSortOrder(context, create_info.sort_keys, *schema, placeholder_sort_order_id);
	metadata->sort_specs.emplace(placeholder_sort_order_id, std::move(placeholder_sort_order));
	metadata->default_sort_order_id = placeholder_sort_order_id;
	return metadata;
}

// CTAS stores columns using Iceberg storage types (e.g. HUGEINT -> DECIMAL(38,0)), which can differ from
// the SELECT output. The write pipeline is typed with the storage types, so without a cast the append fails
// with a type mismatch.
static PhysicalOperator &CastCtasToIcebergStorageTypes(ClientContext &context, PhysicalPlanGenerator &planner,
                                                       PhysicalOperator &plan, const IcebergTableMetadata &metadata) {
	auto &storage_schema = metadata.GetLatestSchema();
	auto &src_types = plan.types;
	D_ASSERT(src_types.size() == storage_schema.columns.size());

	bool needs_cast = false;
	vector<LogicalType> target_types;
	target_types.reserve(src_types.size());
	for (idx_t i = 0; i < src_types.size(); i++) {
		auto &target = storage_schema.columns[i]->type;
		if (target != src_types[i]) {
			needs_cast = true;
		}
		target_types.push_back(target);
	}
	if (!needs_cast) {
		return plan;
	}

	vector<unique_ptr<Expression>> expressions;
	expressions.reserve(src_types.size());
	for (idx_t i = 0; i < src_types.size(); i++) {
		unique_ptr<Expression> expr = make_uniq<BoundReferenceExpression>(src_types[i], i);
		if (target_types[i] != src_types[i]) {
			expr = BoundCastExpression::AddCastToType(context, std::move(expr), target_types[i]);
		}
		expressions.push_back(std::move(expr));
	}
	auto &proj =
	    planner.Make<PhysicalProjection>(std::move(target_types), std::move(expressions), plan.estimated_cardinality);
	proj.children.push_back(plan);
	return proj;
}

PhysicalOperator &IcebergCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                    LogicalCreateTable &op, PhysicalOperator &plan_p) {
	// create a fake local iceberg table with desired columns
	auto placeholder_metadata = BuildPlaceholderMetadata(context, *op.info);
	auto &placeholder_schema = placeholder_metadata->GetLatestSchema();
	auto &plan = CastCtasToIcebergStorageTypes(context, planner, plan_p, *placeholder_metadata);
	IcebergCopyInput copy_input(context, *placeholder_metadata, placeholder_schema, std::move(op.info));
	auto &physical_copy = IcebergInsert::PlanCopyForInsert(context, planner, copy_input, &plan);

	auto &insert = planner.Make<IcebergInsert>(op).Cast<IcebergInsert>();
	// the copy creates the table; the insert reads the resulting entry back out of it
	insert.ctas_copy_op = physical_copy;
	insert.children.push_back(physical_copy);
	return insert;
}

} // namespace duckdb
