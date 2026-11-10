#include "catalog/rest/transaction/iceberg_transaction_manager.hpp"
#include "catalog/rest/transaction/iceberg_written_files.hpp"
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
	auto written_files = IcebergWrittenFilesState::TryGet(context);
	try {
		ic_transaction.Commit();
	} catch (std::exception &ex) {
		// core does not roll back a transaction whose commit failed
		if (written_files) {
			written_files->RollbackTransaction(transaction);
		}
		return ErrorData(ex);
	}
	if (written_files) {
		written_files->CommitTransaction(transaction);
	}
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void IcebergTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &ic_transaction = transaction.Cast<IcebergTransaction>();
	ic_transaction.Rollback();
	auto context = ic_transaction.context.lock();
	if (context) {
		if (auto written_files = IcebergWrittenFilesState::TryGet(*context)) {
			written_files->RollbackTransaction(transaction);
		}
	}
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void IcebergTransactionManager::Checkpoint(ClientContext &context, bool force) {
	return;
}

} // namespace duckdb
