
#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/catalog/iceberg_catalog.hpp"
#include "storage/iceberg_transaction.hpp"

namespace duckdb {

class IcebergTransactionManager : public TransactionManager {
public:
	IcebergTransactionManager(AttachedDatabase &db_p, IcebergCatalog &ic_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	IcebergCatalog &ic_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<IcebergTransaction>> transactions;
};

} // namespace duckdb
