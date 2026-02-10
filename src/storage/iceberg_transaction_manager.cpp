#include "storage/iceberg_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

IcebergTransactionManager::IcebergTransactionManager(AttachedDatabase &db_p, IcebergCatalog &ic_catalog)
    : TransactionManager(db_p), ic_catalog(ic_catalog) {
}

Transaction &IcebergTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<IcebergTransaction>(ic_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData IcebergTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &ic_transaction = transaction.Cast<IcebergTransaction>();
	try {
		ic_transaction.Commit();
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void IcebergTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &ic_transaction = transaction.Cast<IcebergTransaction>();
	ic_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void IcebergTransactionManager::Checkpoint(ClientContext &context, bool force) {
	auto &transaction = IcebergTransaction::Get(context, db.GetCatalog());
	// auto &db = transaction.GetConnection();
	// db.Execute("CHECKPOINT");
}

} // namespace duckdb
