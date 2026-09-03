#include "execution/operator/iceberg_copy_to_file.hpp"

#include "duckdb/common/mutex.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "execution/operator/iceberg_insert.hpp"

namespace duckdb {

IcebergCopyToFile::IcebergCopyToFile(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function,
                                     unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality,
                                     unique_ptr<BoundCreateTableInfo> ctas_info_p)
    : PhysicalCopyToFile(physical_plan, std::move(types), std::move(function), std::move(bind_data),
                         estimated_cardinality),
      ctas_info(std::move(ctas_info_p)) {
	// constant for every Iceberg write path; not derived from IcebergCopyOptions
	use_tmp_file = false;
	parallel = true;
}

void IcebergCopyToFile::ApplyCopyOptions(IcebergCopyOptions &copy_options) {
	bind_data = std::move(copy_options.bind_data);
	file_path = std::move(copy_options.file_path);
	filename_pattern = std::move(copy_options.filename_pattern);
	file_extension = std::move(copy_options.file_extension);
	overwrite_mode = copy_options.overwrite_mode;
	per_thread_output = copy_options.per_thread_output;
	file_size_bytes = copy_options.file_size_bytes;
	batch_size = copy_options.batch_size;
	batch_size_bytes = copy_options.batch_size_bytes;
	return_type = copy_options.return_type;

	partition_output = copy_options.partition_output;
	write_partition_columns = copy_options.write_partition_columns;
	write_empty_file = copy_options.write_empty_file;
	partition_columns = std::move(copy_options.partition_columns);
	names = std::move(copy_options.names);
	expected_types = std::move(copy_options.expected_types);
	hive_file_pattern = copy_options.partitioned_paths;
	// PlanCopyForInsert moves these out into a PhysicalOrder when the write is not partitioned, leaving an
	// empty vector here; for a partitioned write the copy operator sorts within each partition itself.
	order_columns = std::move(copy_options.order_columns);
}

unique_ptr<GlobalSinkState> IcebergCopyToFile::GetGlobalSinkState(ClientContext &context) const {
	// PhysicalCopyToFile reads file_path here to schedule the output directory setup, so for a CTAS the
	// table has to exist - and its copy options have to be installed - before we hand over to the base class.
	EnsureTableCreated(context);
	return PhysicalCopyToFile::GetGlobalSinkState(context);
}

void IcebergCopyToFile::EnsureTableCreated(ClientContext &context) const {
	if (!ctas_info) {
		// every other write path targets a table that already exists, so the plan-time options are final
		return;
	}

	annotated_lock_guard<annotated_mutex> guard(create_lock);
	if (created_table) {
		return;
	}

	// GetGlobalSinkState is const, but the physical plan owns this operator mutably - creating the table
	// and rebinding the copy options is the same work the planner does, only postponed until now.
	auto &op = const_cast<IcebergCopyToFile &>(*this);
	auto &schema_entry = op.ctas_info->schema.Cast<IcebergSchemaEntry>();
	auto &catalog = schema_entry.catalog;
	auto transaction = catalog.GetCatalogTransaction(context);

	auto table = schema_entry.CreateTable(transaction, context, *op.ctas_info);
	if (!table) {
		throw InternalException("Iceberg CTAS: CreateTable request failed");
	}
	auto &ic_table = table->Cast<IcebergTableSchemaVersion>();
	// Load any per-table credentials (e.g. SigV4 for the data location).
	ic_table.PrepareIcebergScanFromEntry(context);

	auto &table_metadata = ic_table.table_info.table_metadata;
	auto &table_schema = table_metadata.GetLatestSchema();

	// Replace the plan-time options, which were derived from placeholder metadata without a location.
	IcebergCopyInput copy_input(context, table_metadata, table_schema);
	auto copy_options = IcebergInsert::GetCopyOptions(context, copy_input);
	op.ApplyCopyOptions(copy_options);

	created_table = &ic_table;
}

optional_ptr<IcebergTableSchemaVersion> IcebergCopyToFile::GetCreatedTable() const {
	annotated_lock_guard<annotated_mutex> guard(create_lock);
	return created_table;
}

string IcebergCopyToFile::GetName() const {
	if (IsCTAS()) {
		return "CREATE TABLE & COPY TO FILE";
	}
	return PhysicalCopyToFile::GetName();
}

InsertionOrderPreservingMap<string> IcebergCopyToFile::ParamsToString() const {
	auto result = PhysicalCopyToFile::ParamsToString();
	if (ctas_info) {
		result["Table Name"] = ctas_info->Base().GetTableName().GetIdentifierName();
	}
	return result;
}

} // namespace duckdb
