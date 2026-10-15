#pragma once

#include "catalog/rest/api/catalog_api.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_executor.hpp"

namespace duckdb {

class IcebergCatalog;
class IcebergSchemaEntry;

//! Best-effort discovery owned by the attached catalog, independent of the attaching connection.
class IcebergCatalogPrefetch {
public:
	IcebergCatalogPrefetch(ClientContext &context, IcebergCatalog &catalog);
	~IcebergCatalogPrefetch();

	void Start();
	void Stop();

private:
	enum class RequestType : uint8_t { SCHEMAS, TABLES, TABLE };
	struct Request {
		RequestType type;
		shared_ptr<IcebergSchemaEntry> schema;
		string table_name;
	};
	class Worker;

	void Run(idx_t worker);
	void Execute(ClientContext &context, const Request &request);
	void Enqueue(vector<Request> requests);
	void ScheduleWorkers() DUCKDB_REQUIRES(lock);

	IcebergCatalog &catalog;
	//! These contexts borrow the database: owning it would create database -> catalog -> context -> database.
	//! The catalog drains all workers before the database or its services can be destroyed.
	vector<shared_ptr<ClientContext>> contexts;
	annotated_mutex lock;
	deque<Request> pending DUCKDB_GUARDED_BY(lock);
	vector<bool> running DUCKDB_GUARDED_BY(lock);
	bool stopped DUCKDB_GUARDED_BY(lock) = false;
	mutex stop_lock;
	//! Destroy the executor before any state referenced by its workers.
	TaskExecutor executor;
};

} // namespace duckdb
