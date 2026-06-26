#include "catalog/rest/transaction/iceberg_transaction_update.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"

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
		// transaction's RENAME overwriting the entry with a different table's metadata.
		// Only override when the original has a known UUID (skip for newly created tables).
		if (!table.table_metadata.table_uuid.empty()) {
			it->second.table_metadata.table_uuid = table.table_metadata.table_uuid;
		}
		it->second.InitSchemaVersions();
	}
	transaction.SetLatestTableState(it->second, IcebergTableStatus::ALIVE);
	return it->second;
}

IcebergTransactionData &IcebergTransactionAlterUpdate::GetOrCreateTransactionData(IcebergTableInformation &table) {
	auto table_key = table.GetTableKey();
	auto it = table_transaction_data.find(table_key);
	if (it == table_transaction_data.end()) {
		auto context = transaction.context.lock();
		it = table_transaction_data.emplace(table_key, make_uniq<IcebergTransactionData>(*context, table)).first;
	}
	return *it->second;
}

optional_ptr<IcebergTransactionData> IcebergTransactionAlterUpdate::GetTransactionData(const string &table_key) const {
	auto it = table_transaction_data.find(table_key);
	if (it == table_transaction_data.end()) {
		return nullptr;
	}
	return it->second.get();
}

optional_ptr<IcebergTransactionData>
IcebergTransactionAlterUpdate::GetTransactionData(const IcebergTableInformation &table) const {
	return GetTransactionData(table.GetTableKey());
}

bool IcebergTransactionAlterUpdate::HasTransactionUpdates(const string &table_key) const {
	auto transaction_data = GetTransactionData(table_key);
	return transaction_data && transaction_data->HasUpdates();
}

bool IcebergTransactionAlterUpdate::HasTransactionUpdates(const IcebergTableInformation &table) const {
	return HasTransactionUpdates(table.GetTableKey());
}

bool IcebergTransactionAlterUpdate::HasUpdates() const {
	for (auto &it : updated_tables) {
		if (HasTransactionUpdates(it.first)) {
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
