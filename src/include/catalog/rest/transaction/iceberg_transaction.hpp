
#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "catalog/rest/iceberg_schema_set.hpp"
#include "catalog/rest/api/iceberg_retry.hpp"
#include "catalog/rest/transaction/iceberg_transaction_update.hpp"

namespace duckdb {
class IcebergCatalog;
class IcebergSchemaEntry;
class IcebergTableEntry;

enum class IcebergTableStatus : uint8_t { ALIVE, DROPPED, RENAMED, MISSING };

struct IcebergTransactionTableState {
public:
	IcebergTransactionTableState();
	explicit IcebergTransactionTableState(shared_ptr<IcebergTableInformation> catalog_table);
	explicit IcebergTransactionTableState(IcebergTableInformation &&transaction_table);

public:
	IcebergTableInformation &GetInfo() {
		if (transaction_table) {
			return *transaction_table;
		}
		if (!catalog_table) {
			throw InternalException("GetInfo called on IcebergTransactionTableState without a table, status: %d",
			                        static_cast<uint8_t>(status));
		}
		return *catalog_table;
	}
	const IcebergTableInformation &GetInfo() const;
	IcebergTableInformation &GetOrCreateTransactionInfo(IcebergTransaction &transaction);

public:
	bool IsDroppedOrRenamed() const {
		return status == IcebergTableStatus::DROPPED || status == IcebergTableStatus::RENAMED;
	}
	bool IsMissing() const {
		return status == IcebergTableStatus::MISSING;
	}
	bool IsAlive() const {
		return status == IcebergTableStatus::ALIVE;
	}
	void SetStatus(IcebergTableStatus value) {
		status = value;
	}

private:
	//! The catalog state is retained as the source for lazily materializing transaction-local state.
	shared_ptr<IcebergTableInformation> catalog_table;
	//! Lazily materialized transaction-local state. Its stable address is referenced by table updates and schema
	//! entries.
	unique_ptr<IcebergTableInformation> transaction_table;
	IcebergTableStatus status;
};

struct SchemaPropertyUpdates {
	case_insensitive_map_t<string> updates;
	set<string> removals;
};

class IcebergTransaction : public Transaction {
public:
	friend struct IcebergTransactionData;
	friend struct IcebergTransactionAlterUpdate;

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
	void DoTableRename(IcebergTransactionRenameUpdate &rename_update, ClientContext &context);
	void DoSchemaCreates(ClientContext &context);
	void DoSchemaDeletes(ClientContext &context);
	void DoSchemaPropertyUpdates(ClientContext &context);
	IcebergCatalog &GetCatalog();
	void DoMultiTableCommitUpdates(IcebergTransactionAlterUpdate &alter_update, ClientContext &context);
	void DoSingleTableCommitUpdates(IcebergTransactionAlterUpdate &alter_update, ClientContext &context);
	optional_ptr<IcebergTransactionTableState> GetLatestTableState(const string &table_key);
	IcebergTransactionTableState &SetCatalogTableState(shared_ptr<IcebergTableInformation> table);
	IcebergTransactionTableState &SetTransactionTableState(const string &table_key, IcebergTableInformation &&table,
	                                                       IcebergTableStatus status);
	IcebergTransactionTableState &GetOrCreateTransactionTableState(const IcebergTableInformation &table);
	IcebergTransactionTableState &SetLatestTableState(const string &table_key, IcebergTableStatus status);
	bool StartedBefore(timestamp_ms_t timestamp_ms) const;
	IcebergTransactionAlterUpdate &GetOrCreateAlter();
	IcebergTableInformation &DeleteTable(IcebergTableInformation &table);
	IcebergTableInformation &RenameTable(IcebergTableInformation &table, const string &new_name);
	bool MultiTableCommitAvailable() const;

private:
	bool HasTableUpdate() const;
	IcebergTransactionAlterUpdate *GetAlterUpdate();
	const IcebergTransactionAlterUpdate *GetAlterUpdate() const;
	bool CanUseMultiTableCommit(const IcebergTransactionAlterUpdate &alter_update) const;
	void VerifyAlterUpdateAtomicity(const IcebergTransactionAlterUpdate &alter_update) const;
	void CleanupMetadataFiles(ClientContext &context, const vector<string> &paths);
	void RefreshRetryTables(IcebergTransactionAlterUpdate &alter_update, const case_insensitive_set_t &table_keys,
	                        ClientContext &context);
	void CleanupFiles();
	//! Evict the touched tables' cached LoadTableResult so a retry after a failed commit (e.g. a 409
	//! conflict) doesn't keep reusing the same stale metadata.
	void EvictCachedTables();
	//! Commit outcome unknown (5xx / no HTTP status); CleanupFiles() then keeps the written files.
	bool commit_state_unknown = false;

private:
	DatabaseInstance &db;
	IcebergCatalog &catalog;
	AccessMode access_mode;

public:
	//! Tables referenced by this transaction that have to stay alive for the duration of the transaction.
	case_insensitive_map_t<shared_ptr<IcebergTableInformation>> tables;
	//! The visible state of every resolved table in this transaction.
	case_insensitive_map_t<IcebergTransactionTableState> current_table_data;
	//! Declared after current_table_data so update references are destroyed before the referenced table states.
	IcebergTransactionUpdate transaction_update;

	unordered_set<string> created_schemas;
	unordered_set<string> deleted_schemas;

	bool called_list_schemas = false;
	//! Set of schemas that this transaction has listed tables for
	case_insensitive_set_t listed_schemas;

	case_insensitive_set_t looked_up_entries;
	mutex lock;

	case_insensitive_map_t<SchemaPropertyUpdates> schema_property_updates;
};

void ApplyTableUpdate(IcebergTableInformation &table_info, IcebergTransaction &iceberg_transaction,
                      const std::function<void(IcebergTableInformation &)> &callback);

} // namespace duckdb
