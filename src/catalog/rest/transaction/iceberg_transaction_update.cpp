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

IcebergTransactionTableState &IcebergTransactionAlterUpdate::GetOrInitializeTable(IcebergTableInformation &table) {
	auto locked_context = transaction.context.lock();
	auto &client_context = *locked_context;

	auto table_key = table.GetTableKey();
	auto it = updated_tables.find(table_key);
	if (it == updated_tables.end()) {
		it = updated_tables.emplace(table_key, IcebergTransactionTableState(table)).first;
		auto metadata = table.GetTransactionStartMetadata(client_context, transaction);
		if (!table.table_metadata.table_uuid.empty()) {
			metadata.table_uuid = table.table_metadata.table_uuid;
		}
		it->second.SetMetadata(std::move(metadata));
	}
	transaction.SetLatestTableState(it->second.GetInfo(), IcebergTableStatus::ALIVE);
	return it->second;
}

IcebergTransactionData &IcebergTransactionAlterUpdate::GetOrCreateTransactionData(IcebergTransactionTableState &table) {
	return table.GetOrCreateTransactionData(transaction);
}

optional_ptr<IcebergTransactionData> IcebergTransactionAlterUpdate::GetTransactionData(const string &table_key) const {
	auto it = updated_tables.find(table_key);
	if (it == updated_tables.end()) {
		return nullptr;
	}
	return it->second.GetTransactionData();
}

optional_ptr<IcebergTransactionData>
IcebergTransactionAlterUpdate::GetTransactionData(const IcebergTransactionTableState &table) const {
	return GetTransactionData(table.GetInfo().GetTableKey());
}

bool IcebergTransactionAlterUpdate::HasTransactionUpdates(const string &table_key) const {
	auto transaction_data = GetTransactionData(table_key);
	return transaction_data && transaction_data->HasUpdates();
}

bool IcebergTransactionAlterUpdate::HasTransactionUpdates(const IcebergTransactionTableState &table) const {
	return HasTransactionUpdates(table.GetInfo().GetTableKey());
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
	auto emplace_res = updated_tables.emplace(table_key, IcebergTransactionTableState(nullptr));
	if (!emplace_res.second) {
		throw InternalException("Table %s was already created somehow?", table_key);
	}
	emplace_res.first->second.SetOwnedTable(std::move(table));
	emplace_res.first->second.SetMetadata(emplace_res.first->second.GetInfo().table_metadata.Copy());

	transaction.current_table_data.emplace(table_key,
	                                       IcebergTransactionTableState(emplace_res.first->second.GetInfo()));
	return emplace_res.first->second.GetInfo();
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
