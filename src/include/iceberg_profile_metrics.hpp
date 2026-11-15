#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class ClientContext;
class ExtensionLoader;
struct MetricsTimer;

//! Query profile metrics of the REST catalog, reported under `iceberg.`
struct IcebergProfileMetrics {
	//! Requests sent to the REST catalog, token requests included
	static constexpr const char *REST_REQUESTS = "iceberg.rest_requests";
	//! Time spent in those requests in seconds, summed over requests that run in parallel
	static constexpr const char *REST_REQUEST_TIME = "iceberg.rest_request_time";

	static void Register(ExtensionLoader &loader);
	//! Counts a request, and returns the timer that adds its time when it goes out of scope
	static MetricsTimer StartRequest(ClientContext &context);
};

} // namespace duckdb
