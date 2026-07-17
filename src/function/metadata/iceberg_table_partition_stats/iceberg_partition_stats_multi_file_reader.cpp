#include "function/metadata/iceberg_table_partition_stats/iceberg_partition_stats_multi_file_reader.hpp"

#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/open_file_info.hpp"

#include "function/metadata/iceberg_table_partition_stats/iceberg_partition_stats_multi_file_list.hpp"

namespace duckdb {

namespace {

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

static MultiFileColumnDefinition CreateColumn(const char *name, const LogicalType &type, bool optional) {
	MultiFileColumnDefinition column(name, type);
	if (optional) {
		column.default_expression = make_uniq<ConstantExpression>(Value(type));
	}
	return column;
}

static vector<MultiFileColumnDefinition> BuildPartitionStatsSchema() {
	vector<MultiFileColumnDefinition> schema;
	schema.reserve(13);
	schema.push_back(CreateColumn(PARTITION_COLUMN, LogicalType::VARCHAR, false));
	schema.push_back(CreateColumn(SPEC_ID_COLUMN, LogicalType::INTEGER, false));
	schema.push_back(CreateColumn(DATA_RECORD_COUNT_COLUMN, LogicalType::BIGINT, false));
	schema.push_back(CreateColumn(DATA_FILE_COUNT_COLUMN, LogicalType::INTEGER, false));
	schema.push_back(CreateColumn(TOTAL_DATA_FILE_SIZE_COLUMN, LogicalType::BIGINT, false));
	schema.push_back(CreateColumn(POSITION_DELETE_RECORD_COUNT_COLUMN, LogicalType::BIGINT, true));
	schema.push_back(CreateColumn(POSITION_DELETE_FILE_COUNT_COLUMN, LogicalType::INTEGER, true));
	schema.push_back(CreateColumn(DV_COUNT_COLUMN, LogicalType::INTEGER, true));
	schema.push_back(CreateColumn(EQUALITY_DELETE_RECORD_COUNT_COLUMN, LogicalType::BIGINT, true));
	schema.push_back(CreateColumn(EQUALITY_DELETE_FILE_COUNT_COLUMN, LogicalType::INTEGER, true));
	schema.push_back(CreateColumn(TOTAL_RECORD_COUNT_COLUMN, LogicalType::BIGINT, true));
	schema.push_back(CreateColumn(LAST_UPDATED_AT_COLUMN, LogicalType::BIGINT, true));
	schema.push_back(CreateColumn(LAST_UPDATED_SNAPSHOT_ID_COLUMN, LogicalType::BIGINT, true));
	return schema;
}

} // namespace

unique_ptr<MultiFileReader> IcebergPartitionStatsMultiFileReader::CreateInstance(const TableFunction &table) {
	return make_uniq<IcebergPartitionStatsMultiFileReader>(table.function_info);
}

shared_ptr<MultiFileList> IcebergPartitionStatsMultiFileReader::CreateFileList(ClientContext &context,
                                                                               const vector<string> &paths,
                                                                               const FileGlobInput &glob_input) {
	auto scan_info = shared_ptr_cast<TableFunctionInfo, IcebergPartitionStatsScanInfo>(function_info);
	vector<OpenFileInfo> open_files;
	open_files.reserve(scan_info->files.size());
	for (auto &file : scan_info->files) {
		open_files.emplace_back(file.path);
		auto &file_info = open_files.back();
		file_info.extended_info = make_uniq<ExtendedOpenFileInfo>();
		file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
		file_info.extended_info->options["file_size"] = Value::UBIGINT(file.file_size_in_bytes);
		file_info.extended_info->options["etag"] = Value("");
		file_info.extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
	}
	return make_shared_ptr<IcebergPartitionStatsMultiFileList>(std::move(scan_info), std::move(open_files));
}

bool IcebergPartitionStatsMultiFileReader::Bind(MultiFileOptions &options, MultiFileList &files,
                                                vector<LogicalType> &return_types, vector<Identifier> &names,
                                                MultiFileReaderBindData &bind_data) {
	auto schema = BuildPartitionStatsSchema();
	for (auto &column : schema) {
		return_types.push_back(column.type);
		names.push_back(Identifier(column.name));
	}
	bind_data.schema = std::move(schema);
	bind_data.mapping = MultiFileColumnMappingMode::BY_NAME;
	return true;
}

void IcebergPartitionStatsMultiFileReader::BindOptions(MultiFileOptions &options, MultiFileList &files,
                                                       vector<LogicalType> &return_types, vector<Identifier> &names,
                                                       MultiFileReaderBindData &bind_data) {
	options.auto_detect_hive_partitioning = false;
	options.hive_partitioning = false;
	options.union_by_name = false;
	MultiFileReader::BindOptions(options, files, return_types, names, bind_data);
}

ReaderInitializeType IcebergPartitionStatsMultiFileReader::CreateMapping(
    ClientContext &context, MultiFileReaderData &reader_data, const vector<MultiFileColumnDefinition> &global_columns,
    const vector<ColumnIndex> &global_column_ids, optional_ptr<TableFilterSet> filters, MultiFileList &multi_file_list,
    const MultiFileReaderBindData &bind_data, const virtual_column_map_t &virtual_columns,
    MultiFileColumnMappingMode mapping_mode) {
	auto result = MultiFileReader::CreateMapping(context, reader_data, global_columns, global_column_ids, filters,
	                                             multi_file_list, bind_data, virtual_columns, mapping_mode);
	if (result != ReaderInitializeType::INITIALIZED) {
		return result;
	}

	auto &local_columns = reader_data.reader->GetColumns();
	optional_idx partition_global_idx;
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto global_column_id = global_column_ids[i].GetPrimaryIndex();
		if (global_columns[global_column_id].name.GetIdentifierName() == PARTITION_COLUMN) {
			partition_global_idx = i;
			break;
		}
	}
	if (!partition_global_idx.IsValid()) {
		return result;
	}

	optional_idx partition_local_column_id;
	for (idx_t i = 0; i < local_columns.size(); i++) {
		if (local_columns[i].name.GetIdentifierName() == PARTITION_COLUMN) {
			partition_local_column_id = i;
			break;
		}
	}
	if (!partition_local_column_id.IsValid()) {
		return result;
	}

	optional_idx partition_projection_idx;
	for (idx_t i = 0; i < reader_data.reader->column_ids.size(); i++) {
		if (reader_data.reader->column_ids[MultiFileLocalIndex(i)].GetId() == partition_local_column_id.GetIndex()) {
			partition_projection_idx = i;
			break;
		}
	}
	if (!partition_projection_idx.IsValid()) {
		return result;
	}

	auto &local_partition_type = local_columns[partition_local_column_id.GetIndex()].type;
	unique_ptr<Expression> partition_expr =
	    make_uniq<BoundReferenceExpression>(local_partition_type, partition_projection_idx.GetIndex());
	partition_expr = BoundCastExpression::AddCastToType(context, std::move(partition_expr), LogicalType::VARCHAR);
	reader_data.expressions[partition_global_idx.GetIndex()] = std::move(partition_expr);
	return result;
}

ReaderInitializeType IcebergPartitionStatsMultiFileReader::CreateMapping(
    ClientContext &context, MultiFileReaderData &reader_data, const vector<MultiFileColumnDefinition> &global_columns,
    const vector<ColumnIndex> &global_column_ids, optional_ptr<TableFilterSet> filters, MultiFileList &multi_file_list,
    const MultiFileReaderBindData &bind_data, const virtual_column_map_t &virtual_columns) {
	return CreateMapping(context, reader_data, global_columns, global_column_ids, filters, multi_file_list, bind_data,
	                     virtual_columns, bind_data.mapping);
}

} // namespace duckdb
