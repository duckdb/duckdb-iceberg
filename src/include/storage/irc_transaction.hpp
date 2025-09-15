
#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "storage/irc_schema_set.hpp"

namespace duckdb {
class IRCatalog;
class IRCSchemaEntry;
class ICTableEntry;

struct TableTransactionInfo {
	TableTransactionInfo() {};

	rest_api_objects::CommitTransactionRequest request;
	// if a table is created with assert create, we cannot use the
	// transactions/commit endpoint. Instead we iterate through each table
	// update and update each table individually
	bool has_assert_create = false;
};

class IRCTransaction : public Transaction {
public:
	IRCTransaction(IRCatalog &ic_catalog, TransactionManager &manager, ClientContext &context);
	~IRCTransaction() override;

public:
	void Start();
	void Commit();
	void Rollback();
	static IRCTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const {
		return access_mode;
	}
	IRCSchemaSet &GetSchemas() {
		return schemas;
	}
	void MarkTableAsDirty(const ICTableEntry &table);
	void MarkTableAsDeleted(const ICTableEntry &table);
	void DoTableUpdates(ClientContext &context);
	void DoTableDeletes(ClientContext &context);
	bool DirtyTablesHaveUpdates();
	IRCatalog &GetCatalog();
	void DropSecrets(ClientContext &context);
	TableTransactionInfo GetTransactionRequest(ClientContext &context);

private:
	void CleanupFiles();

private:
	DatabaseInstance &db;
	IRCatalog &catalog;
	AccessMode access_mode;

public:
	IRCSchemaSet schemas;
	//! Tables marked dirty in this transaction, to be rewritten on commit
	unordered_set<const ICTableEntry *> dirty_tables;
	unordered_set<const ICTableEntry *> deleted_tables;
	case_insensitive_set_t created_secrets;

	case_insensitive_set_t looked_up_entries;
};

} // namespace duckdb
