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
		it = updated_tables.emplace(table_key, table.Copy(transaction)).first;
		// Preserve the table_uuid from the original table info (resolved at transaction start).
		// Copy() reads from the global request cache, which can be contaminated by another
		// transaction's in-flight RENAME overwriting the entry with a different table's metadata.
		it->second.table_metadata.table_uuid = table.table_metadata.table_uuid;
		it->second.InitSchemaVersions();
	}
	transaction.SetLatestTableState(it->second, IcebergTableStatus::ALIVE);
	return it->second;
}

bool IcebergTransactionAlterUpdate::HasUpdates() const {
	for (auto &it : updated_tables) {
		auto &table = it.second;
		if (table.HasTransactionUpdates()) {
			return true;
		}
	}
	return false;
}

IcebergTableInformation &IcebergTransactionAlterUpdate::CreateTable(const string &table_key,
                                                                    IcebergTableInformation &&table) {
	auto emplace_res = updated_tables.emplace(table_key, std::move(table));
	if (!emplace_res.second) {
		throw InternalException("Table %s was already created somehow?", table_key);
	}

	transaction.current_table_data.emplace(table_key, IcebergTransactionTableState(emplace_res.first->second));
	return emplace_res.first->second;
}

IcebergTransactionDeleteUpdate::IcebergTransactionDeleteUpdate(IcebergTransaction &transaction,
                                                               const IcebergTableInformation &table)
    : IcebergTransactionUpdate(transaction, TYPE), deleted_table(table.Copy(transaction)) {
}
IcebergTransactionDeleteUpdate::~IcebergTransactionDeleteUpdate() {
}

IcebergTransactionRenameUpdate::IcebergTransactionRenameUpdate(IcebergTransaction &transaction,
                                                               const IcebergTableInformation &table,
                                                               const string &new_name)
    : IcebergTransactionUpdate(transaction, TYPE), table(table), new_table(table.Copy(transaction)),
      new_name(new_name) {
	new_table.name = new_name;
}
IcebergTransactionRenameUpdate::~IcebergTransactionRenameUpdate() {
}

} // namespace duckdb
