#include "storage/irc_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

ICTransactionManager::ICTransactionManager(AttachedDatabase &db_p, IRCatalog &ic_catalog)
    : TransactionManager(db_p), ic_catalog(ic_catalog) {
}

Transaction &ICTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<IRCTransaction>(ic_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData ICTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &ic_transaction = transaction.Cast<IRCTransaction>();
	try {
		ic_transaction.Commit();
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void ICTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &ic_transaction = transaction.Cast<IRCTransaction>();
	ic_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void ICTransactionManager::Checkpoint(ClientContext &context, bool force) {
	auto &transaction = IRCTransaction::Get(context, db.GetCatalog());
	// auto &db = transaction.GetConnection();
	// db.Execute("CHECKPOINT");
}

} // namespace duckdb
