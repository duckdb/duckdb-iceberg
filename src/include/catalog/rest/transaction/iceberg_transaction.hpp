
#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "catalog/rest/iceberg_schema_set.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

namespace duckdb {
class IcebergCatalog;
class IcebergSchemaEntry;
class IcebergTableEntry;
class IcebergTableSchema;
class BoundAtClause;
struct IcebergTransactionUpdate;
struct IcebergTransactionAlterUpdate;
struct IcebergTransactionDeleteUpdate;
struct IcebergTransactionRenameUpdate;
struct IcebergTransactionData;

struct TableTransactionInfo {
	TableTransactionInfo() {};

	rest_api_objects::CommitTransactionRequest request;
	case_insensitive_map_t<idx_t> table_requests;
	case_insensitive_map_t<vector<string>> created_metadata_files;
	bool retryable = false;
};

enum class IcebergTableStatus : uint8_t { ALIVE, DROPPED, RENAMED, MISSING };

struct IcebergTransactionTableState {
public:
	IcebergTransactionTableState(optional_ptr<IcebergTableInformation> table);

public:
	IcebergTableInformation &GetInfo() {
		if (owned_table) {
			return *owned_table;
		}
		if (!table) {
			throw InternalException("GetInfo called on IcebergTransactionTableState without a table, status: %d",
			                        static_cast<uint8_t>(status));
		}
		return *table;
	}
	const IcebergTableInformation &GetInfo() const {
		if (owned_table) {
			return *owned_table;
		}
		if (!table) {
			throw InternalException("GetInfo called on IcebergTransactionTableState without a table, status: %d",
			                        static_cast<uint8_t>(status));
		}
		return *table;
	}
	const IcebergTableMetadata &GetBaseMetadata() const {
		return GetInfo().table_metadata;
	}
	IcebergTableMetadata GetTransactionMetadata() const;
	optional_ptr<IcebergTransactionData> GetTransactionData() const {
		return transaction_data.get();
	}
	IcebergTransactionData &GetOrCreateTransactionData(IcebergTransaction &transaction);
	optional_ptr<CatalogEntry> GetSchemaVersion(ClientContext &context, optional_ptr<BoundAtClause> at);
	optional_ptr<CatalogEntry> GetLatestSchema(ClientContext &context);
	IcebergTableEntry &GetOrCreateSchemaEntry(const IcebergTableSchema &table_schema);
	IcebergTableEntry &GetOrCreateDummyEntry();

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
	bool HasOwnedTable() const {
		return owned_table != nullptr;
	}
	bool IsTransactionLocalTable() const {
		return transaction_local_table;
	}
	void SetStatus(IcebergTableStatus value) {
		status = value;
	}
	void SetTable(IcebergTableInformation &value) {
		if (owned_table && owned_table.get() == &value) {
			table = *owned_table;
			return;
		}
		schema_versions.clear();
		dummy_entry.reset();
		transaction_data.reset();
		owned_table.reset();
		transaction_local_table = false;
		table = value;
	}
	void SetOwnedTable(IcebergTableInformation &&value, bool transaction_local = false) {
		schema_versions.clear();
		dummy_entry.reset();
		transaction_data.reset();
		owned_table = make_uniq<IcebergTableInformation>(std::move(value));
		transaction_local_table = transaction_local;
		table = *owned_table;
	}

private:
	optional_ptr<IcebergTableInformation> table;
	unique_ptr<IcebergTableInformation> owned_table;
	IcebergTableStatus status;
	bool transaction_local_table = false;
	unordered_map<int32_t, unique_ptr<IcebergTableEntry>> schema_versions;
	unique_ptr<IcebergTableEntry> dummy_entry;
	unique_ptr<IcebergTransactionData> transaction_data;
};

struct SchemaPropertyUpdates {
	case_insensitive_map_t<string> updates;
	set<string> removals;
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
	timestamp_t GetTransactionStartTimestamp() const {
		return transaction_start_timestamp;
	}
	void DoTableUpdates(IcebergTransactionAlterUpdate &alter_update, ClientContext &context);
	void DoTableDeletes(IcebergTransactionDeleteUpdate &delete_update, ClientContext &context);
	void DoTableRename(IcebergTransactionRenameUpdate &rename_update, ClientContext &context);
	void DoSchemaCreates(ClientContext &context);
	void DoSchemaDeletes(ClientContext &context);
	void DoSchemaPropertyUpdates(ClientContext &context);
	IcebergCatalog &GetCatalog();
	void DropSecrets(ClientContext &context);
	TableTransactionInfo GetTransactionRequest(IcebergTransactionAlterUpdate &alter_update,
	                                           case_insensitive_map_t<IcebergTableInformation> &staged_tables,
	                                           ClientContext &context);
	void DoMultiTableCommitUpdates(IcebergTransactionAlterUpdate &alter_update, ClientContext &context);
	void DoSingleTableCommitUpdates(IcebergTransactionAlterUpdate &alter_update, ClientContext &context);
	optional_ptr<IcebergTransactionTableState> GetLatestTableState(const string &table_key);
	IcebergTransactionTableState &SetLatestTableState(IcebergTableInformation &table, IcebergTableStatus status);
	IcebergTransactionTableState &SetLatestTableState(const string &table_key, IcebergTableStatus status);
	bool StartedBefore(timestamp_t timestamp_ms) const;
	IcebergTransactionAlterUpdate &GetOrCreateAlter();
	optional_ptr<IcebergTransactionData> GetTransactionData(const string &table_key) const;
	bool HasTransactionUpdates(const string &table_key) const;
	IcebergTableInformation &DeleteTable(IcebergTableInformation &table);
	IcebergTableInformation &RenameTable(IcebergTableInformation &table, const string &new_name);

private:
	bool CanUseMultiTableCommit(const IcebergTransactionAlterUpdate &alter_update) const;
	void CleanupMetadataFiles(ClientContext &context, const vector<string> &paths);
	void RefreshRetryTables(IcebergTransactionAlterUpdate &alter_update,
	                        case_insensitive_map_t<IcebergTableInformation> &retry_tables, ClientContext &context);
	void CleanupFiles();

private:
	DatabaseInstance &db;
	IcebergCatalog &catalog;
	AccessMode access_mode;
	timestamp_t transaction_start_timestamp;

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
                      const std::function<void(IcebergTransactionTableState &, IcebergTransactionData &)> &callback);

} // namespace duckdb
