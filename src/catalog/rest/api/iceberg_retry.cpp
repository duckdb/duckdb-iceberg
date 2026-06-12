#include "catalog/rest/api/iceberg_retry.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"

#include <string>

namespace duckdb {

namespace {

constexpr const char *NUM_RETRIES = "commit.retry.num-retries";
constexpr const char *MIN_WAIT_MS = "commit.retry.min-wait-ms";
constexpr const char *MAX_WAIT_MS = "commit.retry.max-wait-ms";
constexpr const char *TOTAL_WAIT_MS = "commit.retry.total-timeout-ms";

constexpr idx_t NUM_RETRIES_DEFAULT = 4;
constexpr int64_t MIN_WAIT_MS_DEFAULT = 100;
constexpr int64_t MAX_WAIT_MS_DEFAULT = 60 * 1000;
constexpr int64_t TOTAL_WAIT_MS_DEFAULT = 30 * 60 * 1000;

template <class T>
T ParseInt(const string &value, T fallback, bool allow_zero) {
	if (value.empty()) {
		return fallback;
	}
	try {
		auto parsed = std::stoll(value);
		if (parsed < 0 || (parsed == 0 && !allow_zero)) {
			return fallback;
		}
		return static_cast<T>(parsed);
	} catch (...) {
		return fallback;
	}
}

} // namespace

IcebergRetryConfig IcebergRetryConfig::FromTableMetadata(const IcebergTableMetadata &metadata) {
	IcebergRetryConfig config;
	//! num-retries may legitimately be 0 (no retries, single attempt).
	config.num_retries = ParseInt<idx_t>(metadata.GetTableProperty(NUM_RETRIES), NUM_RETRIES_DEFAULT, true);
	config.min_wait_ms = ParseInt<int64_t>(metadata.GetTableProperty(MIN_WAIT_MS), MIN_WAIT_MS_DEFAULT, false);
	config.max_wait_ms = ParseInt<int64_t>(metadata.GetTableProperty(MAX_WAIT_MS), MAX_WAIT_MS_DEFAULT, false);
	config.total_wait_ms = ParseInt<int64_t>(metadata.GetTableProperty(TOTAL_WAIT_MS), TOTAL_WAIT_MS_DEFAULT, false);
	//! Guard against a misconfigured table where min > max.
	if (config.min_wait_ms > config.max_wait_ms) {
		config.min_wait_ms = config.max_wait_ms;
	}
	return config;
}

IcebergRetryConfig IcebergRetryConfig::MostLenient(const IcebergRetryConfig &other) const {
	//! Multiple tables committed in one atomic transaction share a single retry loop. Take the most
	//! retry-tolerant policy so no table's (more permissive) configuration is silently dropped: the
	//! larger num-retries / max-wait / total-timeout, and the smaller min-wait.
	IcebergRetryConfig merged;
	merged.num_retries = MaxValue<idx_t>(num_retries, other.num_retries);
	merged.min_wait_ms = MinValue<int64_t>(min_wait_ms, other.min_wait_ms);
	merged.max_wait_ms = MaxValue<int64_t>(max_wait_ms, other.max_wait_ms);
	merged.total_wait_ms = MaxValue<int64_t>(total_wait_ms, other.total_wait_ms);
	//! Preserve the min<=max invariant FromTableMetadata guarantees (both inputs satisfy it, but the
	//! cross-table min/max combination must too).
	if (merged.min_wait_ms > merged.max_wait_ms) {
		merged.min_wait_ms = merged.max_wait_ms;
	}
	return merged;
}

int64_t IcebergRetryConfig::BackoffMs(idx_t attempt) const {
	//! min_wait * 2^attempt, clamped to max_wait. The doubling is guarded against signed-int64
	//! overflow: table properties are user-supplied, so a large min/max-wait must not trigger UB.
	//! Once `wait` would exceed max_wait_ms after doubling, we are already going to clamp anyway.
	int64_t wait = min_wait_ms;
	for (idx_t i = 0; i < attempt && wait < max_wait_ms; i++) {
		if (wait > max_wait_ms / 2) {
			//! Doubling would reach/exceed max_wait_ms (and could overflow); clamp and stop.
			wait = max_wait_ms;
			break;
		}
		wait *= 2;
	}
	if (wait > max_wait_ms) {
		wait = max_wait_ms;
	}
	return wait;
}

int64_t IcebergRetryConfig::BackoffMsWithJitter(idx_t attempt, double unit_random) const {
	auto base = BackoffMs(attempt);
	if (base <= 0) {
		return 0;
	}
	//! Clamp the random fraction defensively, then scale: result in [0, base]. Full jitter spreads
	//! retries uniformly across the window so a herd of concurrent committers does not re-collide.
	if (unit_random < 0.0) {
		unit_random = 0.0;
	} else if (unit_random > 1.0) {
		unit_random = 1.0;
	}
	return static_cast<int64_t>(static_cast<double>(base) * unit_random);
}

int64_t IcebergRetryConfig::DecorrelatedBackoffMs(int64_t prev_sleep_ms, double unit_random) const {
	if (unit_random < 0.0) {
		unit_random = 0.0;
	} else if (unit_random > 1.0) {
		unit_random = 1.0;
	}
	//! Lower bound is min_wait; upper bound is prev_sleep*3, both clamped to max_wait. Guard the *3
	//! against int64 overflow (prev_sleep is bounded by max_wait, which is user-supplied).
	int64_t lo = min_wait_ms;
	int64_t hi;
	if (prev_sleep_ms > max_wait_ms / 3) {
		hi = max_wait_ms;
	} else {
		hi = prev_sleep_ms * 3;
	}
	if (hi > max_wait_ms) {
		hi = max_wait_ms;
	}
	if (hi < lo) {
		hi = lo; // degenerate config (min > max already normalized in FromTableMetadata, but be safe)
	}
	int64_t span = hi - lo;
	return lo + static_cast<int64_t>(static_cast<double>(span) * unit_random);
}

} // namespace duckdb
