#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

struct IcebergTableMetadata;

//! Resolved `commit.retry.*` table properties controlling the optimistic-concurrency retry loop.
//! Defaults mirror Apache Iceberg Java (the authoritative reference).
struct IcebergRetryConfig {
	idx_t num_retries = 4;                  // commit.retry.num-retries (default 4 -> 5 attempts total)
	int64_t min_wait_ms = 100;              // commit.retry.min-wait-ms
	int64_t max_wait_ms = 60 * 1000;        // commit.retry.max-wait-ms
	int64_t total_wait_ms = 30 * 60 * 1000; // commit.retry.total-timeout-ms

	static IcebergRetryConfig FromTableMetadata(const IcebergTableMetadata &metadata);

	//! Combine two configs into the most lenient (most retry-tolerant) of the two, taking the
	//! larger num-retries / max-wait / total-timeout and the smaller min-wait. Used when a single
	//! transaction commits multiple tables atomically: the retry loop is shared, so no table's
	//! (more permissive) policy should be silently dropped in favor of an arbitrary "first" table.
	IcebergRetryConfig MostLenient(const IcebergRetryConfig &other) const;

	//! Decorrelated jitter (AWS "Exponential Backoff And Jitter"): the next sleep is drawn from
	//! [min_wait, prev_sleep*3], clamped to max_wait. Starting `prev_sleep` is min_wait, so the first
	//! retries are TIGHT (near min_wait) -- a conflict's refresh+recommit window is short, so retrying
	//! quickly usually wins -- and the window only widens as repeated failures accumulate, which is
	//! exactly when a thundering herd needs spreading. `unit_random` in [0,1). Returns the new sleep;
	//! the caller feeds it back as `prev_sleep` next iteration. Pure/unit-testable.
	int64_t DecorrelatedBackoffMs(int64_t prev_sleep_ms, double unit_random) const;
};

} // namespace duckdb
