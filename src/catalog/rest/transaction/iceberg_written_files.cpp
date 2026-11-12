#include "catalog/rest/transaction/iceberg_written_files.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {

static constexpr const char *ICEBERG_WRITTEN_FILES_KEY = "iceberg_written_files";

shared_ptr<IcebergWrittenFilesState> IcebergWrittenFilesState::Get(ClientContext &context) {
	return context.registered_state->GetOrCreate<IcebergWrittenFilesState>(ICEBERG_WRITTEN_FILES_KEY);
}

shared_ptr<IcebergWrittenFilesState> IcebergWrittenFilesState::TryGet(ClientContext &context) {
	return context.registered_state->Get<IcebergWrittenFilesState>(ICEBERG_WRITTEN_FILES_KEY);
}

void IcebergWrittenFilesState::BeginWrite() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	write_started = true;
}

void IcebergWrittenFilesState::AddFiles(vector<IcebergWrittenFile> written, Transaction &transaction) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	for (auto &file : written) {
		pending_files.push_back(std::move(file));
	}
	pending_transaction = &transaction;
}

vector<IcebergWrittenFile> IcebergWrittenFilesState::GetFiles() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return files;
}

void IcebergWrittenFilesState::IcebergTransactionCommitted(Transaction &transaction) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (pending_transaction.get() == &transaction) {
		pending_transaction = nullptr;
	}
	if (files_transaction.get() == &transaction) {
		files_transaction = nullptr;
	}
}

void IcebergWrittenFilesState::IcebergTransactionRolledBack(Transaction &transaction) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (pending_transaction.get() == &transaction) {
		pending_files.clear();
		pending_transaction = nullptr;
	}
	if (files_transaction.get() == &transaction) {
		files.clear();
		files_transaction = nullptr;
	}
}

void IcebergWrittenFilesState::QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (!write_started) {
		return;
	}
	write_started = false;
	if (!error || !error->HasError()) {
		files = std::move(pending_files);
		files_transaction = pending_transaction;
	} else {
		files.clear();
		files_transaction = nullptr;
	}
	pending_files.clear();
	pending_transaction = nullptr;
}

} // namespace duckdb
