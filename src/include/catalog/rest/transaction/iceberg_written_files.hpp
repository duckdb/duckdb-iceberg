#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/main/client_context_state.hpp"

namespace duckdb {

class Transaction;

struct IcebergWrittenFile {
	string file_path;
	int64_t record_count = 0;
	int64_t file_size_in_bytes = 0;
};

//! The data files the connection's last INSERT or CREATE TABLE AS into an Iceberg table added. When such a statement
//! ends, its files replace the list; if it failed while running, the list is empty. Files of an Iceberg transaction
//! that rolls back or fails to commit are dropped.
class IcebergWrittenFilesState : public ClientContextState {
public:
	static shared_ptr<IcebergWrittenFilesState> Get(ClientContext &context);
	static shared_ptr<IcebergWrittenFilesState> TryGet(ClientContext &context);

	void BeginWrite();
	//! 'transaction' is the Iceberg transaction that staged the files
	void AddFiles(vector<IcebergWrittenFile> files, Transaction &transaction);
	vector<IcebergWrittenFile> GetFiles();
	//! Called by the Iceberg transaction manager once 'transaction' committed
	void IcebergTransactionCommitted(Transaction &transaction);
	//! Called by the Iceberg transaction manager when 'transaction' rolls back or fails to commit
	void IcebergTransactionRolledBack(Transaction &transaction);

	void QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) override;

private:
	annotated_mutex lock;
	bool write_started DUCKDB_GUARDED_BY(lock) = false;
	vector<IcebergWrittenFile> pending_files DUCKDB_GUARDED_BY(lock);
	//! The open transaction that staged the pending files, or nullptr once it committed
	optional_ptr<Transaction> pending_transaction DUCKDB_GUARDED_BY(lock);
	vector<IcebergWrittenFile> files DUCKDB_GUARDED_BY(lock);
	//! The open transaction the files belong to, or nullptr when they are committed
	optional_ptr<Transaction> files_transaction DUCKDB_GUARDED_BY(lock);
};

} // namespace duckdb
