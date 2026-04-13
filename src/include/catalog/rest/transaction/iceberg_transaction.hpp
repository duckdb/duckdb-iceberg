
#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "catalog/rest/iceberg_schema_set.hpp"

namespace duckdb {
class IcebergCatalog;
class IcebergSchemaEntry;
class IcebergTableEntry;
struct IcebergTransactionUpdate;
struct IcebergTransactionAlterUpdate;
struct IcebergTransactionDeleteUpdate;

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
	    : sequence_number(sequence_number), snapshot_id(snapshot_id), exists(true) {
	}
	TableInfoCache(bool exists_) : sequence_number(0), snapshot_id(0), exists(exists_) {
	}
	idx_t sequence_number;
	idx_t snapshot_id;
	bool exists;
};

enum class IcebergTableStatus : uint8_t { ALIVE, DROPPED };

enum class IcebergTableSource : uint8_t {
	//! Loaded from external source
	EXTERNAL,
	//! Version that exists within the transaction
	TRANSACTION
};

struct IcebergTransactionTableState {
public:
	IcebergTransactionTableState(IcebergTableInformation &table, IcebergTableSource source);

public:
	reference<IcebergTableInformation> table;
	IcebergTableSource source;
	IcebergTableStatus status;
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
	void DoTableUpdates(IcebergTransactionAlterUpdate &alter_update, ClientContext &context);
	void DoTableDeletes(IcebergTransactionDeleteUpdate &delete_update, ClientContext &context);
	void DoSchemaCreates(ClientContext &context);
	void DoSchemaDeletes(ClientContext &context);
	IcebergCatalog &GetCatalog();
	void DropSecrets(ClientContext &context);
	TableTransactionInfo GetTransactionRequest(IcebergTransactionAlterUpdate &alter_update, ClientContext &context);
	void RecordTableRequest(const string &table_key, idx_t sequence_number, idx_t snapshot_id);
	void RecordTableRequest(const string &table_key);
	TableInfoCache GetTableRequestResult(const string &table_key);
	optional_ptr<IcebergTransactionTableState> GetLatestTableState(const string &table_key);
	IcebergTransactionTableState &SetLatestTableState(IcebergTableInformation &table, IcebergTableSource source);
	bool StartedBefore(timestamp_t timestamp_ms) const;
	IcebergTransactionAlterUpdate &GetOrCreateAlter();
	IcebergTableInformation &DeleteTable(const IcebergTableInformation &table);

private:
	void CleanupFiles();

private:
	DatabaseInstance &db;
	IcebergCatalog &catalog;
	AccessMode access_mode;
	//! Tables that have been requested in the current transaction
	//! and do not need to be requested again. When we request, we also
	//! store the latest snapshot id, so if the table is requested again
	//! (with no updates), we can return table information at that snapshot
	//! while other transactions can still request up to date tables
	case_insensitive_map_t<TableInfoCache> requested_tables;

public:
	//! Tables referenced by this transaction that have to stay alive for the duration of the transaction.
	case_insensitive_map_t<shared_ptr<IcebergTableInformation>> tables;
	vector<unique_ptr<IcebergTransactionUpdate>> transaction_updates;
	//! The latest state of a table (either points into 'transaction_updates' or 'tables')
	case_insensitive_map_t<IcebergTransactionTableState> current_table_data;

	unordered_set<string> created_schemas;
	unordered_set<string> deleted_schemas;

	bool called_list_schemas = false;
	//! Set of schemas that this transaction has listed tables for
	case_insensitive_set_t listed_schemas;

	case_insensitive_set_t created_secrets;
	case_insensitive_set_t looked_up_entries;
	mutex lock;
};

void ApplyTableUpdate(IcebergTableInformation &table_info, IcebergTransaction &iceberg_transaction,
                      const std::function<void(IcebergTableInformation &)> &callback);

} // namespace duckdb
