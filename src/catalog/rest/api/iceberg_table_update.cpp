#include "catalog/rest/api/iceberg_table_update.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"

namespace duckdb {

IcebergCommitState::IcebergCommitState(const IcebergTableInformation &table_info, ClientContext &context)
    : table_info(table_info), context(context) {
	//! Baseline (sequence number / row id) is taken from the current table metadata. A retry
	//! reconstructs IcebergCommitState from the refreshed metadata, so this runs once per attempt.
	auto &metadata = table_info.table_metadata;
	next_sequence_number = metadata.last_sequence_number + 1;
	next_row_id = metadata.has_next_row_id ? metadata.next_row_id : 0;
}

IcebergTableUpdate::IcebergTableUpdate(IcebergTableUpdateType type, const IcebergTableInformation &table_info)
    : type(type), table_info(table_info) {
}

} // namespace duckdb
