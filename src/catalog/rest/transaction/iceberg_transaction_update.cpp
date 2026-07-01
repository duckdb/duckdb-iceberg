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

static IcebergTableInformation CopyTransactionTableInfo(const IcebergTableInformation &source) {
	auto result = IcebergTableInformation(source.catalog, source.schema, source.name);
	result.table_id = source.table_id;
	result.table_metadata = source.table_metadata.Copy();
	result.config = source.config;
	result.storage_credentials = source.storage_credentials;
	result.latest_metadata_json = source.latest_metadata_json;
	return result;
}

IcebergTransactionTableState &IcebergTransactionAlterUpdate::GetOrInitializeTable(IcebergTableInformation &table) {
	auto locked_context = transaction.context.lock();
	auto &client_context = *locked_context;

	auto table_key = table.GetTableKey();
	auto it = updated_tables.find(table_key);
	if (it == updated_tables.end()) {
		auto latest_state = transaction.GetLatestTableState(table_key);
		if (latest_state && latest_state->IsTransactionLocalTable()) {
			auto owned_table = CopyTransactionTableInfo(latest_state->GetInfo());
			owned_table.table_metadata = latest_state->GetTransactionMetadata();
			it = updated_tables.emplace(table_key, IcebergTransactionTableState(nullptr)).first;
			it->second.SetOwnedTable(std::move(owned_table), true);
			it->second.SetStatus(IcebergTableStatus::ALIVE);
		} else {
			auto metadata = table.GetTransactionStartMetadata(client_context, transaction);
			if (!table.table_metadata.table_uuid.empty()) {
				metadata.table_uuid = table.table_metadata.table_uuid;
			}
			auto owned_table = CopyTransactionTableInfo(table);
			owned_table.table_metadata = std::move(metadata);
			it = updated_tables.emplace(table_key, IcebergTransactionTableState(nullptr)).first;
			it->second.SetOwnedTable(std::move(owned_table));
			it->second.SetStatus(IcebergTableStatus::ALIVE);
		}
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
	emplace_res.first->second.SetOwnedTable(std::move(table), true);
	emplace_res.first->second.SetStatus(IcebergTableStatus::ALIVE);

	transaction.current_table_data.emplace(table_key,
	                                       IcebergTransactionTableState(emplace_res.first->second.GetInfo()));
	return emplace_res.first->second.GetInfo();
}

IcebergTransactionDeleteUpdate::IcebergTransactionDeleteUpdate(IcebergTransaction &transaction,
                                                               const IcebergTableInformation &table)
    : IcebergTransactionDeleteUpdate(transaction, table.Copy(transaction)) {
}

IcebergTransactionDeleteUpdate::IcebergTransactionDeleteUpdate(IcebergTransaction &transaction,
                                                               IcebergTableInformation &&table)
    : IcebergTransactionUpdate(transaction, TYPE), deleted_table(std::move(table)) {
}
IcebergTransactionDeleteUpdate::~IcebergTransactionDeleteUpdate() {
}

IcebergTransactionRenameUpdate::IcebergTransactionRenameUpdate(IcebergTransaction &transaction,
                                                               const IcebergTableInformation &table,
                                                               const string &new_name)
    : IcebergTransactionRenameUpdate(transaction, table.Copy(transaction), new_name) {
}

IcebergTransactionRenameUpdate::IcebergTransactionRenameUpdate(IcebergTransaction &transaction,
                                                               IcebergTableInformation &&table, const string &new_name)
    : IcebergTransactionUpdate(transaction, TYPE), source_namespace_items(table.schema.namespace_items),
      source_name(table.name), new_table(std::move(table)), new_name(new_name) {
	new_table.name = new_name;
}
IcebergTransactionRenameUpdate::~IcebergTransactionRenameUpdate() {
}

} // namespace duckdb
