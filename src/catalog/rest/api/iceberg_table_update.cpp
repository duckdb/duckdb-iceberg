#include "catalog/rest/api/iceberg_table_update.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"

namespace duckdb {

IcebergCommitState::IcebergCommitState(const IcebergTableInformation &table_info, ClientContext &context)
    : table_info(table_info), context(context) {
	next_sequence_number = table_info.table_metadata.last_sequence_number + 1;
	if (table_info.table_metadata.has_next_row_id) {
		next_row_id = table_info.table_metadata.next_row_id;
	}
}

IcebergTableUpdate::IcebergTableUpdate(IcebergTableUpdateType type, const IcebergTableInformation &table_info)
    : type(type), table_info(table_info) {
}

} // namespace duckdb
