#include "catalog/rest/api/iceberg_table_update.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"

namespace duckdb {

IcebergCommitState::IcebergCommitState(const IcebergTableInformation &table_info, ClientContext &context)
    : table_info(table_info), context(context) {
}

IcebergTableUpdate::IcebergTableUpdate(IcebergTableUpdateType type, const IcebergTableInformation &table_info)
    : type(type), table_info(table_info) {
}

} // namespace duckdb
