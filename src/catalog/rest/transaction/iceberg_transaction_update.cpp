#include "catalog/rest/transaction/iceberg_transaction_update.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"

namespace duckdb {

IcebergTransactionUpdate::IcebergTransactionUpdate(IcebergTransaction &transaction, IcebergTransactionUpdateType type)
    : transaction(transaction), type(type) {
}
IcebergTransactionUpdate::~IcebergTransactionUpdate() {
}

IcebergTransactionAlterUpdate::IcebergTransactionAlterUpdate(IcebergTransaction &transaction)
    : IcebergTransactionUpdate(transaction, TYPE) {
}
IcebergTransactionAlterUpdate::~IcebergTransactionAlterUpdate() {
}

IcebergTableInformation &IcebergTransactionAlterUpdate::GetOrInitializeTable(const IcebergTableInformation &table) {
	auto table_key = table.GetTableKey();
	auto it = updated_tables.find(table_key);
	if (it == updated_tables.end()) {
		it = updated_tables.emplace(table_key, table.Copy()).first;
		it->second.InitSchemaVersions();
	}

	transaction.SetLatestTableState(it->second, IcebergTableSource::TRANSACTION);
	return it->second;
}

IcebergTableInformation &IcebergTransactionAlterUpdate::CreateTable(const string &table_key,
                                                                    IcebergTableInformation &&table) {
	auto emplace_res = updated_tables.emplace(table_key, std::move(table));
	if (!emplace_res.second) {
		throw InternalException("Table %s was already created somehow?", table_key);
	}

	transaction.current_table_data.emplace(
	    table_key, IcebergTransactionTableState(emplace_res.first->second, IcebergTableSource::TRANSACTION));
	return emplace_res.first->second;
}

IcebergTransactionDeleteUpdate::IcebergTransactionDeleteUpdate(IcebergTransaction &transaction,
                                                               const IcebergTableInformation &table)
    : IcebergTransactionUpdate(transaction, TYPE), table(table) {
}
IcebergTransactionDeleteUpdate::~IcebergTransactionDeleteUpdate() {
}

} // namespace duckdb
