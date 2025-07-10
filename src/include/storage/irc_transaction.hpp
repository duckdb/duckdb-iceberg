
#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "storage/irc_schema_set.hpp"

namespace duckdb {
class IRCatalog;
class IRCSchemaEntry;
class ICTableEntry;

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
	void MarkTableAsNew(const ICTableEntry &table);
	IRCatalog &GetCatalog();
	// stage create = false, table is created immediately in the IRC
	// stage create = true, table is not created, but metadata is initialized and returned
	//     To commit table, call CommitNewTable again with stage_create = true
	rest_api_objects::LoadTableResult CommitNewTable(ClientContext &context, const ICTableEntry *table,
	                                                 bool stage_create = false);
	void DropSecrets(ClientContext &context);
	rest_api_objects::CommitTransactionRequest GetTransactionRequest(ClientContext &context);

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
	unordered_set<const ICTableEntry *> new_tables;
	case_insensitive_set_t created_secrets;
};

} // namespace duckdb
