#include "catalog/rest/iceberg_metadata_prefetch.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_executor.hpp"

namespace duckdb {

class IcebergMetadataPrefetch::FetchTask : public BaseExecutorTask {
public:
	FetchTask(TaskExecutor &executor, ClientContext &context, Entry &entry)
	    : BaseExecutorTask(executor), context(context), entry(entry) {
	}

	void ExecuteTask() override {
		try {
			if (context.IsInterrupted()) {
				throw InterruptException();
			}
			auto &table = *entry.table;
			entry.result =
			    make_uniq<IcebergLoadTableResult>(IRCAPI::GetTable(context, table.catalog, table.schema, table.name));
		} catch (std::exception &ex) {
			// A speculative failure must not fail a query that never consumes this table.
			entry.error = ErrorData(ex);
		} catch (...) {
			entry.error = ErrorData("Unknown error while prefetching Iceberg table metadata");
		}
	}

private:
	ClientContext &context;
	Entry &entry;
};

void IcebergMetadataPrefetch::Register(shared_ptr<IcebergTable> table) {
	lock_guard<mutex> guard(lock);
	auto key = table->GetTableKey();
	if (entries.find(key) != entries.end()) {
		return;
	}
	auto entry = make_uniq<Entry>(std::move(table));
	auto inserted = entries.emplace(std::move(key), std::move(entry));
	pending.emplace_back(*inserted.first->second);
}

void IcebergMetadataPrefetch::Prefetch(ClientContext &context, const string &table_key) {
	lock_guard<mutex> guard(lock);
	auto it = entries.find(table_key);
	if (it == entries.end() || it->second->requested) {
		return;
	}

	// Use async workers for blocking HTTP requests, plus the caller that drains the batch.
	// With no async workers this falls back to one request at a time.
	auto &scheduler = TaskScheduler::GetScheduler(context);
	const auto batch_size = MinValue<idx_t>(7, scheduler.NumberOfAsyncThreads()) + 1;
	vector<reference<Entry>> batch;
	batch.emplace_back(*it->second);
	it->second->requested = true;
	while (batch.size() < batch_size && next_pending < pending.size()) {
		auto &entry = pending[next_pending++].get();
		if (!entry.requested) {
			entry.requested = true;
			batch.emplace_back(entry);
		}
	}

	TaskExecutor executor(context, TaskSchedulerType::ASYNC);
	try {
		for (auto &ref : batch) {
			auto &entry = ref.get();
			auto &catalog = entry.table->catalog;
			// Check cache validity on the consuming thread. FillEntry will use the cache directly;
			// there is no need to copy cached metadata into the prefetch buffer.
			if (catalog.attach_options.max_table_staleness_micros.IsValid() &&
			    catalog.table_request_cache.Get(context, entry.table->GetTableKey(),
			                                    [](const rest_api_objects::LoadTableResult &) {})) {
				continue;
			}
			executor.ScheduleTask(make_uniq<FetchTask>(executor, context, entry));
		}
		executor.WorkOnTasks();
	} catch (...) {
		executor.CancelAndDrain();
		// Allow a later statement in the transaction to retry work that never completed.
		for (auto &ref : batch) {
			auto &entry = ref.get();
			if (!entry.result && !entry.error.HasError()) {
				entry.requested = false;
			}
		}
		throw;
	}
	if (context.IsInterrupted()) {
		throw InterruptException();
	}
}

unique_ptr<IcebergLoadTableResult> IcebergMetadataPrefetch::Take(const string &table_key) {
	lock_guard<mutex> guard(lock);
	auto it = entries.find(table_key);
	if (it == entries.end()) {
		return nullptr;
	}
	auto &entry = *it->second;
	entry.requested = true;
	if (entry.error.HasError()) {
		entry.error.Throw();
	}
	if (entry.result && entry.result->error_) {
		// Multiple system-table scans may consume the same failing entry. Preserve the response
		// so a second consumer does not issue another request after the first one throws.
		auto result = make_uniq<IcebergLoadTableResult>();
		result->status_ = entry.result->status_;
		result->error_ = entry.result->error_->Copy();
		return result;
	}
	return std::move(entry.result);
}

} // namespace duckdb
