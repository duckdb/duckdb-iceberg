#include "catalog/rest/iceberg_catalog_prefetch.hpp"

#include "catalog/rest/iceberg_catalog.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

class IcebergCatalogPrefetch::Worker : public BaseExecutorTask {
public:
	Worker(IcebergCatalogPrefetch &prefetch, idx_t worker)
	    : BaseExecutorTask(prefetch.executor), prefetch(prefetch), worker(worker) {
	}

	void ExecuteTask() override {
		prefetch.Run(worker);
	}

	string TaskType() const override {
		return "IcebergCatalogPrefetch";
	}

private:
	IcebergCatalogPrefetch &prefetch;
	idx_t worker;
};

IcebergCatalogPrefetch::IcebergCatalogPrefetch(ClientContext &context, IcebergCatalog &catalog)
    : catalog(catalog), executor(TaskScheduler::GetScheduler(context), TaskSchedulerType::ASYNC) {
	auto &scheduler = TaskScheduler::GetScheduler(context);
	// No caller helps background discovery. With no workers, retain ordinary on-demand lookup.
	auto regular_threads = scheduler.NumberOfThreads();
	auto external_threads = Settings::Get<ExternalThreadsSetting>(context);
	auto regular_workers = regular_threads > external_threads ? regular_threads - external_threads : 0;
	auto worker_count = MinValue<idx_t>(8, regular_workers + scheduler.NumberOfAsyncThreads());
	for (idx_t i = 0; i < worker_count; i++) {
		auto borrowed_db = shared_ptr<DatabaseInstance>(shared_ptr<DatabaseInstance>(), context.db.get());
		auto worker_context = make_shared_ptr<ClientContext>(std::move(borrowed_db));
		worker_context->config.user_settings = context.config.user_settings;
		contexts.push_back(std::move(worker_context));
		running.push_back(false);
	}
}

IcebergCatalogPrefetch::~IcebergCatalogPrefetch() {
	Stop();
}

void IcebergCatalogPrefetch::Start() {
	if (!contexts.empty() && catalog.supported_urls.count("GET /v1/{prefix}/namespaces")) {
		Enqueue({{RequestType::SCHEMAS, nullptr, ""}});
	}
}

void IcebergCatalogPrefetch::Stop() {
	lock_guard<mutex> stop_guard(stop_lock);
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		stopped = true;
		pending.clear();
	}
	for (auto &context : contexts) {
		context->Interrupt();
	}
	executor.CancelAndDrain();
}

void IcebergCatalogPrefetch::ScheduleWorkers() {
	auto available = pending.size();
	for (idx_t i = 0; i < running.size() && available > 0; i++) {
		if (running[i]) {
			continue;
		}
		running[i] = true;
		try {
			executor.ScheduleTask(make_uniq<Worker>(*this, i));
		} catch (...) {
			running[i] = false;
			throw;
		}
		available--;
	}
}

void IcebergCatalogPrefetch::Enqueue(vector<Request> requests) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (stopped) {
		return;
	}
	for (auto &request : requests) {
		pending.push_back(std::move(request));
	}
	ScheduleWorkers();
}

void IcebergCatalogPrefetch::Run(idx_t worker) {
	auto &context = *contexts[worker];
	while (true) {
		Request request;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			if (stopped || pending.empty()) {
				running[worker] = false;
				return;
			}
			request = std::move(pending.front());
			pending.pop_front();
		}
		try {
			context.InterruptCheck();
			// Auth/HTTP secret lookup requires a transaction, private to this worker.
			context.transaction.BeginTransaction();
			Execute(context, request);
			context.transaction.Commit();
		} catch (std::exception &ex) {
			if (context.HasActiveTransaction()) {
				context.transaction.Rollback(nullptr);
			}
			if (!context.IsInterrupted()) {
				DUCKDB_LOG_WARNING(context, "Could not prefetch Iceberg catalog '%s': %s", catalog.attach_options.name,
				                   ex.what());
			}
		}
		context.logger->Flush();
	}
}

void IcebergCatalogPrefetch::Execute(ClientContext &context, const Request &request) {
	switch (request.type) {
	case RequestType::SCHEMAS: {
		auto schemas = IcebergListSchemasRequest({}).Execute(context, catalog);
		context.InterruptCheck();
		auto entries = catalog.schemas.AddPrefetchedEntries(std::move(schemas));
		if (!catalog.supported_urls.count("GET /v1/{prefix}/namespaces/{namespace}/tables")) {
			return;
		}
		vector<Request> requests;
		for (auto &entry : entries) {
			requests.push_back({RequestType::TABLES, std::move(entry), ""});
		}
		Enqueue(std::move(requests));
		return;
	}
	case RequestType::TABLES: {
		auto tables = IcebergListTablesRequest(request.schema->namespace_items).Execute(context, catalog);
		context.InterruptCheck();
		if (!tables) {
			return;
		}
		request.schema->tables.AddPrefetchedEntries(*tables);
		if (catalog.attach_options.table_resolution != IcebergTableResolution::EAGER ||
		    !catalog.supported_urls.count("GET /v1/{prefix}/namespaces/{namespace}/tables/{table}")) {
			return;
		}
		vector<Request> requests;
		for (auto &table : *tables) {
			requests.push_back({RequestType::TABLE, request.schema, std::move(table.name)});
		}
		Enqueue(std::move(requests));
		return;
	}
	case RequestType::TABLE: {
		auto key = IcebergTable::GetTableKey(catalog, request.schema->namespace_items, request.table_name);
		auto publication = catalog.table_request_cache.BeginLoad(key, true);
		if (!publication) {
			return;
		}
		auto result =
		    IcebergLoadTableRequest(request.schema->namespace_items, request.table_name).Execute(context, catalog);
		context.InterruptCheck();
		if (result.error_) {
			DUCKDB_LOG_WARNING(context, "Could not prefetch Iceberg table '%s': %s", key,
			                   result.error_->_error.message);
			return;
		}
		// A foreground load or write supersedes this publication. Reuse still obeys MAX_TABLE_STALENESS.
		publication->TryPublish(std::move(result.result_));
		return;
	}
	}
}

} // namespace duckdb
