#include "iceberg_profile_metrics.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/profiler/metric_info.hpp"
#include "duckdb/main/profiler/profiling_utils.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

void IcebergProfileMetrics::Register(ExtensionLoader &loader) {
	loader.RegisterMetric(
	    {REST_REQUESTS, "uint64",
	     "Number of requests sent to the Iceberg REST catalog. A request that DuckDB's HTTP transport "
	     "sends again is not counted twice",
	     "requests"});
	loader.RegisterMetric({REST_REQUEST_TIME, "double",
	                       "Time spent in Iceberg REST catalog requests, summed over all requests", "seconds"});
}

MetricsTimer IcebergProfileMetrics::StartRequest(ClientContext &context) {
	auto &profiler = QueryProfiler::Get(context);
	profiler.AddToMetricCounter(REST_REQUESTS, 1);
	return profiler.StartTimerInternal(REST_REQUEST_TIME);
}

} // namespace duckdb
