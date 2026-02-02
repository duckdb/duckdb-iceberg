
#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "storage/catalog/iceberg_schema_set.hpp"

namespace duckdb {
class IcebergCatalog;
class IcebergSchemaEntry;
class IcebergTableEntry;

struct TableTransactionInfo {
	TableTransactionInfo() {};

	rest_api_objects::CommitTransactionRequest request;
	// if a table is created with assert create, we cannot use the
	// transactions/commit endpoint. Instead we iterate through each table
	// update and update each table individually
	bool has_assert_create = false;
};

struct TableInfoCache {
	TableInfoCache(idx_t sequence_number, idx_t snapshot_id)
	    : sequence_number(sequence_number), snapshot_id(snapshot_id) {
	}
	idx_t sequence_number;
	idx_t snapshot_id;
};

class IcebergTransaction : public Transaction {
public:
	IcebergTransaction(IcebergCatalog &ic_catalog, TransactionManager &manager, ClientContext &context);
	~IcebergTransaction() override;

public:
	void Start();
	void Commit();
	void Rollback();
	static IcebergTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const {
		return access_mode;
	}
	void DoTableUpdates(ClientContext &context);
	void DoTableDeletes(ClientContext &context);
	IcebergCatalog &GetCatalog();
	void DropSecrets(ClientContext &context);
	TableTransactionInfo GetTransactionRequest(ClientContext &context);

private:
	void CleanupFiles();

private:
	DatabaseInstance &db;
	IcebergCatalog &catalog;
	AccessMode access_mode;

public:
	//! tables that have been created in this transaction
	//! tables are hashed by catalog_name.table name
	//! Tables that have been updated in this transaction, to be rewritten on commit.
	case_insensitive_map_t<IcebergTableInformation> updated_tables;
	//! tables that have been deleted in this transaction, to be deleted on commit.
	case_insensitive_map_t<IcebergTableInformation> deleted_tables;
	//! Tables that have been requested in the current transaction
	//! and do not need to be requested again. When we request, we also
	//! store the latest snapshot id, so if the table is requested again
	//! (with no updates), we can return table information at that snapshot
	//! while other transactions can still request up to date tables
	case_insensitive_map_t<TableInfoCache> requested_tables;

	bool called_list_schemas = false;
	case_insensitive_set_t listed_schemas;

	case_insensitive_set_t created_secrets;
	case_insensitive_set_t looked_up_entries;
};

template <typename Callback>
void ApplyTableUpdate(IcebergTableInformation &table_info, IcebergTransaction &iceberg_transaction, Callback callback) {
	if (table_info.IsTransactionLocalTable(iceberg_transaction)) {
		callback(table_info);
	} else {
		iceberg_transaction.updated_tables.emplace(table_info.GetTableKey(), table_info.Copy(iceberg_transaction));
		auto &updated_table = iceberg_transaction.updated_tables.at(table_info.GetTableKey());
		updated_table.InitSchemaVersions();
		callback(updated_table);
	}
}

} // namespace duckdb
