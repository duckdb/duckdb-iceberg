#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

struct IcebergTableMetadata;

//! Resolved `commit.retry.*` table properties controlling the optimistic-concurrency retry loop.
//! Defaults mirror Apache Iceberg Java (the authoritative reference).
struct IcebergRetryConfig {
	idx_t num_retries;     // commit.retry.num-retries (default 4 -> 5 attempts total)
	int64_t min_wait_ms;   // commit.retry.min-wait-ms (default 100)
	int64_t max_wait_ms;   // commit.retry.max-wait-ms (default 60000)
	int64_t total_wait_ms; // commit.retry.total-timeout-ms (default 1800000)

	static IcebergRetryConfig FromTableMetadata(const IcebergTableMetadata &metadata);

	//! Combine two configs into the most lenient (most retry-tolerant) of the two, taking the
	//! larger num-retries / max-wait / total-timeout and the smaller min-wait. Used when a single
	//! transaction commits multiple tables atomically: the retry loop is shared, so no table's
	//! (more permissive) policy should be silently dropped in favor of an arbitrary "first" table.
	IcebergRetryConfig MostLenient(const IcebergRetryConfig &other) const;

	//! Exponential backoff (factor 2) for the given 0-based attempt index, clamped to [min, max].
	//! Pure function, unit-testable. This is the *base* wait, before jitter.
	int64_t BackoffMs(idx_t attempt) const;

	//! Base backoff with full random jitter applied: returns a value in [0, BackoffMs(attempt)].
	//! `unit_random` must be in [0,1) (e.g. from a uniform_real_distribution). Full jitter (AWS
	//! "equal jitter"/"full jitter" literature) is what de-synchronizes a thundering herd of
	//! concurrent writers (thousands of Lambdas) so they do not all wake and re-collide in lockstep.
	//! Kept as a pure function (random source injected) so it stays unit-testable.
	int64_t BackoffMsWithJitter(idx_t attempt, double unit_random) const;

	//! Decorrelated jitter (AWS "Exponential Backoff And Jitter"): the next sleep is drawn from
	//! [min_wait, prev_sleep*3], clamped to max_wait. Starting `prev_sleep` is min_wait, so the first
	//! retries are TIGHT (near min_wait) -- a conflict's refresh+recommit window is short, so retrying
	//! quickly usually wins -- and the window only widens as repeated failures accumulate, which is
	//! exactly when a thundering herd needs spreading. `unit_random` in [0,1). Returns the new sleep;
	//! the caller feeds it back as `prev_sleep` next iteration. Pure/unit-testable.
	int64_t DecorrelatedBackoffMs(int64_t prev_sleep_ms, double unit_random) const;
};

} // namespace duckdb
