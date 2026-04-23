
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
struct IcebergTransactionRenameUpdate;

struct TableTransactionInfo {
	TableTransactionInfo() {};

	rest_api_objects::CommitTransactionRequest request;
	case_insensitive_map_t<idx_t> table_requests;

	// if a table is created with assert create, we cannot use the
	// transactions/commit endpoint. Instead we iterate through each table
	// update and update each table individually
	bool has_assert_create = false;
};

enum class IcebergTableStatus : uint8_t { ALIVE, DROPPED, RENAMED, MISSING };

struct IcebergTransactionTableState {
public:
	IcebergTransactionTableState(optional_ptr<IcebergTableInformation> table);

public:
	IcebergTableInformation &GetInfo() {
		if (!table) {
			throw InternalException("GetInfo called on IcebergTransactionTableState without a table, status: %d",
			                        static_cast<uint8_t>(status));
		}
		return *table;
	}

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
	void SetTable(IcebergTableInformation &value) {
		table = value;
	}

private:
	optional_ptr<IcebergTableInformation> table;
	IcebergTableStatus status;
};

struct SchemaPropertyUpdates {
	case_insensitive_map_t<string> schema_property_updates;
	set<string> schema_property_removals;
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
	void DoTableRename(IcebergTransactionRenameUpdate &rename_update, ClientContext &context);
	void DoSchemaCreates(ClientContext &context);
	void DoSchemaDeletes(ClientContext &context);
	void DoSchemaPropertyUpdates(ClientContext &context);
	IcebergCatalog &GetCatalog();
	void DropSecrets(ClientContext &context);
	TableTransactionInfo GetTransactionRequest(IcebergTransactionAlterUpdate &alter_update, ClientContext &context);
	optional_ptr<IcebergTransactionTableState> GetLatestTableState(const string &table_key);
	IcebergTransactionTableState &SetLatestTableState(IcebergTableInformation &table, IcebergTableStatus status);
	IcebergTransactionTableState &SetLatestTableState(const string &table_key, IcebergTableStatus status);
	bool StartedBefore(timestamp_t timestamp_ms) const;
	IcebergTransactionAlterUpdate &GetOrCreateAlter();
	IcebergTableInformation &DeleteTable(IcebergTableInformation &table);
	IcebergTableInformation &RenameTable(IcebergTableInformation &table, const string &new_name);

private:
	void CleanupFiles();

private:
	DatabaseInstance &db;
	IcebergCatalog &catalog;
	AccessMode access_mode;

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

	case_insensitive_map_t<SchemaPropertyUpdates> schema_property_updates;
};

void ApplyTableUpdate(IcebergTableInformation &table_info, IcebergTransaction &iceberg_transaction,
                      const std::function<void(IcebergTableInformation &)> &callback);

} // namespace duckdb
