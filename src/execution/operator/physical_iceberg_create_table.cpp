#include "execution/operator/physical_iceberg_create_table.hpp"

#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "execution/operator/iceberg_insert.hpp"

namespace duckdb {

PhysicalIcebergCreateTable::PhysicalIcebergCreateTable(PhysicalPlan &physical_plan, IcebergSchemaEntry &schema_entry,
                                                       unique_ptr<BoundCreateTableInfo> info,
                                                       shared_ptr<IcebergCTASCreateState> create_state,
                                                       PhysicalCopyToFile &copy_to_file_op, vector<LogicalType> types,
                                                       idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      schema_entry(schema_entry), info(std::move(info)), create_state(std::move(create_state)),
      copy_to_file_op(copy_to_file_op) {
}

unique_ptr<GlobalOperatorState> PhysicalIcebergCreateTable::GetGlobalOperatorState(ClientContext &context) const {
	auto global_state = make_uniq<IcebergCreateTableGlobalState>();
	MakeCreateTableRequest(context, *global_state);
	return std::move(global_state);
}

void PhysicalIcebergCreateTable::MakeCreateTableRequest(ClientContext &client_context,
                                                        IcebergCreateTableGlobalState &gstate) const {
	std::call_once(gstate.init_flag, [&]() {
		auto &catalog = schema_entry.catalog;
		auto transaction = catalog.GetCatalogTransaction(client_context);

		auto table = schema_entry.CreateTable(transaction, client_context, *info);
		if (!table) {
			throw InternalException("Iceberg CTAS: CreateTable request failed");
		}
		auto &ic_table = table->Cast<IcebergTableEntry>();
		// Load any per-table credentials (e.g. SigV4 for the data location).
		ic_table.PrepareIcebergScanFromEntry(client_context);

		auto &table_metadata = ic_table.table_info.table_metadata;
		auto &table_schema = table_metadata.GetLatestSchema();

		IcebergCopyInput copy_input(client_context, table_metadata, table_schema);
		auto copy_options = IcebergInsert::GetCopyOptions(client_context, copy_input);

		auto &copy_op = copy_to_file_op.get();
		copy_op.bind_data = std::move(copy_options.bind_data);
		copy_op.file_path = std::move(copy_options.file_path);
		copy_op.filename_pattern = std::move(copy_options.filename_pattern);
		copy_op.file_extension = std::move(copy_options.file_extension);
		copy_op.overwrite_mode = copy_options.overwrite_mode;
		copy_op.per_thread_output = copy_options.per_thread_output;
		copy_op.file_size_bytes = copy_options.file_size_bytes;
		copy_op.return_type = copy_options.return_type;
		copy_op.partition_output = copy_options.partition_output;
		copy_op.write_partition_columns = copy_options.write_partition_columns;
		copy_op.write_empty_file = copy_options.write_empty_file;
		copy_op.partition_columns = std::move(copy_options.partition_columns);
		copy_op.names = std::move(copy_options.names);
		copy_op.expected_types = std::move(copy_options.expected_types);

		{
			lock_guard<mutex> guard(create_state->lock);
			create_state->table_entry = &ic_table;
			create_state->created = true;
		}
	});
}

OperatorResultType PhysicalIcebergCreateTable::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                       GlobalOperatorState &gstate_p, OperatorState &state) const {
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalIcebergCreateTable::GetName() const {
	return "ICEBERG_CREATE_TABLE";
}

InsertionOrderPreservingMap<string> PhysicalIcebergCreateTable::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	if (info) {
		result["Table Name"] = info->Base().table;
	}
	return result;
}

} // namespace duckdb
