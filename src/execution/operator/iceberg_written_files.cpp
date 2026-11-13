#include "execution/operator/iceberg_written_files.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {

vector<IcebergWrittenFile> IcebergWrittenFiles::Collect(const vector<IcebergManifestEntry> &entries) {
	vector<IcebergWrittenFile> files;
	files.reserve(entries.size());
	for (auto &entry : entries) {
		auto &data_file = entry.data_file;
		files.push_back({data_file.file_path, data_file.record_count, data_file.file_size_in_bytes});
	}
	return files;
}

static const char *WriteOperationName(IcebergWriteOperation operation) {
	switch (operation) {
	case IcebergWriteOperation::INSERT:
		return "INSERT";
	case IcebergWriteOperation::CTAS:
		return "CTAS";
	case IcebergWriteOperation::UPDATE:
		return "UPDATE";
	case IcebergWriteOperation::MERGE_INSERT:
		return "MERGE_INSERT";
	case IcebergWriteOperation::MERGE_UPDATE:
		return "MERGE_UPDATE";
	case IcebergWriteOperation::COPY_ON_WRITE_DELETE:
		return "COPY_ON_WRITE_DELETE";
	case IcebergWriteOperation::REWRITE_DATA_FILES:
		return "REWRITE_DATA_FILES";
	default:
		throw InternalException("Unrecognized Iceberg write operation");
	}
}

void IcebergWrittenFiles::Record(ClientContext &context, const IcebergTable &table, IcebergWriteOperation operation,
                                 const vector<IcebergWrittenFile> &files) {
	if (files.empty()) {
		return;
	}
	auto table_name = StringUtil::Format("%s.%s.%s", table.catalog.GetName().GetIdentifierName(),
	                                     table.schema.name.GetIdentifierName(), table.name);
	const string operation_name = WriteOperationName(operation);
	// A transaction may stage several statements. Its start timestamp would give all of them the same time.
	const timestamp_tz_t written_at(Timestamp::GetCurrentTimestamp());
	if (IsEnabled(context)) {
		Append(context, table_name, operation_name, written_at, files);
	}
	for (auto &file : files) {
		DUCKDB_LOG(context, IcebergDataFileWrittenLogType, table_name, file.file_path, file.record_count,
		           file.file_size_in_bytes, operation_name, written_at);
	}
}

bool IcebergWrittenFiles::IsEnabled(ClientContext &context) {
	Value enabled;
	return context.TryGetCurrentSetting(SETTING, enabled) && !enabled.IsNull() && BooleanValue::Get(enabled);
}

void IcebergWrittenFiles::Append(ClientContext &context, const string &table, const string &operation,
                                 timestamp_tz_t written_at, const vector<IcebergWrittenFile> &files) {
	// a query may finalize more than one write at once
	static mutex append_lock;
	lock_guard<mutex> guard(append_lock);

	auto &temp_catalog = Catalog::GetCatalog(context, Identifier::TempCatalog());
	QualifiedName name(Identifier::TempCatalog(), Identifier::DefaultSchema(), Identifier(TABLE));
	auto info = make_uniq<CreateTableInfo>(name);
	info->temporary = true;
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	info->columns.AddColumn(ColumnDefinition("query_id", LogicalType::UBIGINT));
	info->columns.AddColumn(ColumnDefinition("table", LogicalType::VARCHAR));
	info->columns.AddColumn(ColumnDefinition("file_path", LogicalType::VARCHAR));
	info->columns.AddColumn(ColumnDefinition("record_count", LogicalType::BIGINT));
	info->columns.AddColumn(ColumnDefinition("file_size_in_bytes", LogicalType::BIGINT));
	info->columns.AddColumn(ColumnDefinition("operation", LogicalType::VARCHAR));
	info->columns.AddColumn(ColumnDefinition("written_at", LogicalType::TIMESTAMP_TZ));
	auto expected_columns = info->columns.Copy();
	temp_catalog.CreateTable(context, std::move(info));

	auto &entry = temp_catalog.GetEntry<TableCatalogEntry>(context, name);
	// The user can create or alter this table. InternalAppender appends by physical position, so even
	// two swapped VARCHAR columns would silently corrupt the reported table names and file paths.
	auto &columns = entry.GetColumns();
	bool compatible = columns.LogicalColumnCount() == expected_columns.LogicalColumnCount();
	for (idx_t i = 0; compatible && i < expected_columns.LogicalColumnCount(); i++) {
		auto &column = columns.GetColumn(LogicalIndex(i));
		auto &expected = expected_columns.GetColumn(LogicalIndex(i));
		compatible =
		    !column.Generated() && column.GetName() == expected.GetName() && column.GetType() == expected.GetType();
	}
	if (!compatible) {
		throw InvalidInputException("temp.main.%s has an incompatible schema. Drop or rename it before enabling %s.",
		                            TABLE, SETTING);
	}
	InternalAppender appender(context, entry);
	auto query_id = context.transaction.GetActiveQuery();
	for (auto &file : files) {
		appender.BeginRow();
		appender.Append<uint64_t>(query_id);
		appender.Append(string_t(table));
		appender.Append(string_t(file.file_path));
		appender.Append<int64_t>(file.record_count);
		appender.Append<int64_t>(file.file_size_in_bytes);
		appender.Append(string_t(operation));
		appender.Append(Value::TIMESTAMPTZ(written_at));
		appender.EndRow();
	}
	appender.Close();
}

} // namespace duckdb
