#include "catalog/rest/iceberg_metadata_prefetch.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include <condition_variable>
#include <chrono>

namespace duckdb {

class IcebergMetadataPrefetch::State {
public:
	enum class Status { QUEUED, RUNNING, READY };
	struct Entry {
		explicit Entry(shared_ptr<IcebergTable> table) : table(std::move(table)) {
		}
		shared_ptr<IcebergTable> table;
		mutex lock;
		std::condition_variable ready;
		Status status = Status::QUEUED;
		unique_ptr<IcebergLoadTableResult> result;
		ErrorData error;
	};

	class FetchTask : public BaseExecutorTask {
	public:
		FetchTask(TaskExecutor &executor, ClientContext &context, Entry &entry)
		    : BaseExecutorTask(executor), context(context), entry(entry) {
		}
		void ExecuteTask() override {
			Fetch(context, entry);
		}

	private:
		ClientContext &context;
		Entry &entry;
	};

	static void Fetch(ClientContext &context, Entry &entry) {
		{
			lock_guard<mutex> guard(entry.lock);
			if (entry.status != Status::QUEUED) {
				return;
			}
			entry.status = Status::RUNNING;
		}
		unique_ptr<IcebergLoadTableResult> result;
		ErrorData error;
		try {
			if (context.IsInterrupted()) {
				throw InterruptException();
			}
			auto &table = *entry.table;
			result =
			    make_uniq<IcebergLoadTableResult>(IRCAPI::GetTable(context, table.catalog, table.schema, table.name));
		} catch (std::exception &ex) {
			// Speculative failures are only reported if this table is consumed.
			error = ErrorData(ex);
		} catch (...) {
			error = ErrorData("Unknown error while prefetching Iceberg table metadata");
		}
		{
			lock_guard<mutex> guard(entry.lock);
			entry.result = std::move(result);
			entry.error = std::move(error);
			entry.status = Status::READY;
		}
		entry.ready.notify_all();
	}

	void Stop() {
		lock_guard<mutex> guard(lock);
		// Cancel unclaimed work and wait for running HTTP requests before releasing the
		// query context or table/schema owners. Never hold an entry lock while draining.
		if (executor) {
			executor->CancelAndDrain();
			executor.reset();
		}
		for (auto &item : entries) {
			auto &entry = *item.second;
			lock_guard<mutex> guard(entry.lock);
			if (entry.error.HasError() && entry.error.Type() == ExceptionType::INTERRUPT) {
				entry.error = ErrorData();
				entry.status = Status::QUEUED;
			}
		}
	}

	mutex lock;
	case_insensitive_map_t<unique_ptr<Entry>> entries;
	unique_ptr<TaskExecutor> executor;
};

class IcebergMetadataPrefetch::QueryState : public ClientContextState {
public:
	void Register(shared_ptr<State> state) {
		lock_guard<mutex> guard(lock);
		states.emplace_back(std::move(state));
	}
	void QueryEnd() override {
		vector<weak_ptr<State>> pending;
		{
			lock_guard<mutex> guard(lock);
			pending.swap(states);
		}
		for (auto &ref : pending) {
			auto state = ref.lock();
			if (state) {
				state->Stop();
			}
		}
	}
	mutex lock;
	vector<weak_ptr<State>> states;
};

IcebergMetadataPrefetch::IcebergMetadataPrefetch() : state(make_shared_ptr<State>()) {
}

IcebergMetadataPrefetch::~IcebergMetadataPrefetch() {
	CancelAndDrain();
}

void IcebergMetadataPrefetch::CancelAndDrain() {
	state->Stop();
}

void IcebergMetadataPrefetch::Register(ClientContext &context, shared_ptr<IcebergTable> table) {
	lock_guard<mutex> guard(state->lock);
	auto key = table->GetTableKey();
	if (state->entries.find(key) != state->entries.end()) {
		return;
	}
	// Check the cache on the caller; workers do not publish catalog state.
	auto &catalog = table->catalog;
	if (catalog.attach_options.max_table_staleness_micros.IsValid() &&
	    catalog.table_request_cache.Get(context, key, [](const rest_api_objects::LoadTableResult &) {})) {
		return;
	}
	if (!state->executor) {
		auto query_state = context.registered_state->GetOrCreate<QueryState>("iceberg_metadata_prefetch");
		query_state->Register(state);
		state->executor = make_uniq<TaskExecutor>(context, TaskSchedulerType::ASYNC);
	}
	auto entry = make_uniq<State::Entry>(std::move(table));
	auto &ref = *entry;
	state->entries.emplace(std::move(key), std::move(entry));
	state->executor->ScheduleTask(make_uniq<State::FetchTask>(*state->executor, context, ref));
}

unique_ptr<IcebergLoadTableResult> IcebergMetadataPrefetch::Take(ClientContext &context, const string &table_key) {
	optional_ptr<State::Entry> entry_ptr;
	{
		lock_guard<mutex> guard(state->lock);
		auto it = state->entries.find(table_key);
		if (it == state->entries.end()) {
			return nullptr;
		}
		entry_ptr = it->second.get();
	}
	auto &entry = *entry_ptr;
	// Claim queued work ourselves. Its scheduler task will see RUNNING/READY and do nothing.
	// This also provides progress when there are no async workers.
	State::Fetch(context, entry);
	std::unique_lock<mutex> guard(entry.lock);
	while (entry.status == State::Status::RUNNING) {
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
		entry.ready.wait_for(guard, std::chrono::milliseconds(10));
	}
	if (entry.error.HasError()) {
		entry.error.Throw();
	}
	if (entry.result && entry.result->error_) {
		// Keep failed responses for repeated consumers in the same transaction.
		auto result = make_uniq<IcebergLoadTableResult>();
		result->status_ = entry.result->status_;
		result->error_ = entry.result->error_->Copy();
		return result;
	}
	return std::move(entry.result);
}

} // namespace duckdb
