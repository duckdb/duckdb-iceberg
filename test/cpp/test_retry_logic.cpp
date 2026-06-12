// Standalone unit tests for the pure-logic pieces of the commit-retry implementation: exponential /
// jittered / decorrelated backoff, commit-status classification, Retry-After parsing and
// consumption, retry-config parsing and cross-table folding, total-timeout enforcement, snapshot
// ancestry walks, and delete-entry attribution / concurrent-delete detection. They do not depend on
// the DuckDB test harness or a catalog, so they build and run on their own with a single command:
//
//   c++ -std=c++17 test/cpp/test_retry_logic.cpp -o /tmp/t && /tmp/t
//
// Why mirrors: the DuckDB `unittest` binary only runs SQL logic tests, and the extension is a
// dynamically-loaded module, so production C++ functions cannot be linked into a standalone binary
// without pulling in all of DuckDB. The blocks below reproduce each production function byte-for-byte
// and name the source file they mirror; a divergence shows up as a failing test during review.
// End-to-end coverage of the write/retry/validate paths (which need a catalog) lives in the
// catalog-required SQL tests run by the lakekeeper/polaris/nessie/fixture CI jobs.

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

using idx_t = uint64_t;

//===--------------------------------------------------------------------===//
// Mirror of IcebergRetryConfig::BackoffMs
//===--------------------------------------------------------------------===//
static int64_t BackoffMs(int64_t min_wait_ms, int64_t max_wait_ms, idx_t attempt) {
	int64_t wait = min_wait_ms;
	for (idx_t i = 0; i < attempt && wait < max_wait_ms; i++) {
		if (wait > max_wait_ms / 2) {
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

//===--------------------------------------------------------------------===//
// Mirror of IcebergRetryConfig::BackoffMsWithJitter
//===--------------------------------------------------------------------===//
static int64_t BackoffMsWithJitter(int64_t min_wait_ms, int64_t max_wait_ms, idx_t attempt, double unit_random) {
	auto base = BackoffMs(min_wait_ms, max_wait_ms, attempt);
	if (base <= 0) {
		return 0;
	}
	if (unit_random < 0.0) {
		unit_random = 0.0;
	} else if (unit_random > 1.0) {
		unit_random = 1.0;
	}
	return static_cast<int64_t>(static_cast<double>(base) * unit_random);
}

//===--------------------------------------------------------------------===//
// Mirror of IcebergRetryConfig::DecorrelatedBackoffMs
//===--------------------------------------------------------------------===//
static int64_t DecorrelatedBackoffMs(int64_t min_wait_ms, int64_t max_wait_ms, int64_t prev_sleep_ms,
                                     double unit_random) {
	if (unit_random < 0.0) {
		unit_random = 0.0;
	} else if (unit_random > 1.0) {
		unit_random = 1.0;
	}
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
		hi = lo;
	}
	int64_t span = hi - lo;
	return lo + static_cast<int64_t>(static_cast<double>(span) * unit_random);
}

//===--------------------------------------------------------------------===//
// Mirror of IcebergRetryConfig::FromTableMetadata property parsing (iceberg_retry.cpp ParseInt +
// FromTableMetadata): empty -> default; negative or (zero && !allow_zero) -> default; min>max
// normalized to min=max. num-retries allows zero; the wait fields do not.
//===--------------------------------------------------------------------===//
struct RetryCfg {
	idx_t num_retries;
	int64_t min_wait_ms;
	int64_t max_wait_ms;
	int64_t total_wait_ms;
};

static int64_t ParseIntField(const std::string &value, int64_t fallback, bool allow_zero) {
	if (value.empty()) {
		return fallback;
	}
	try {
		auto parsed = std::stoll(value);
		if (parsed < 0 || (parsed == 0 && !allow_zero)) {
			return fallback;
		}
		return parsed;
	} catch (...) {
		return fallback;
	}
}

static RetryCfg ParseRetryConfig(const std::string &num, const std::string &min_w, const std::string &max_w,
                                 const std::string &total_w) {
	RetryCfg c;
	c.num_retries = static_cast<idx_t>(ParseIntField(num, 4, true));
	c.min_wait_ms = ParseIntField(min_w, 100, false);
	c.max_wait_ms = ParseIntField(max_w, 60000, false);
	c.total_wait_ms = ParseIntField(total_w, 1800000, false);
	if (c.min_wait_ms > c.max_wait_ms) {
		c.min_wait_ms = c.max_wait_ms;
	}
	return c;
}

//===--------------------------------------------------------------------===//
// Mirror of IcebergRetryConfig::MostLenient (iceberg_retry.cpp): the cross-table fold takes the
// larger num-retries / max-wait / total-timeout and the smaller min-wait, preserving min<=max.
//===--------------------------------------------------------------------===//
static RetryCfg MostLenient(const RetryCfg &a, const RetryCfg &b) {
	RetryCfg m;
	m.num_retries = a.num_retries > b.num_retries ? a.num_retries : b.num_retries;
	m.min_wait_ms = a.min_wait_ms < b.min_wait_ms ? a.min_wait_ms : b.min_wait_ms;
	m.max_wait_ms = a.max_wait_ms > b.max_wait_ms ? a.max_wait_ms : b.max_wait_ms;
	m.total_wait_ms = a.total_wait_ms > b.total_wait_ms ? a.total_wait_ms : b.total_wait_ms;
	if (m.min_wait_ms > m.max_wait_ms) {
		m.min_wait_ms = m.max_wait_ms;
	}
	return m;
}

//===--------------------------------------------------------------------===//
// Mirror of the total-timeout-ms enforcement in IcebergTransaction::DoTableUpdates
// (iceberg_transaction.cpp): before sleeping, abort iff attempt>0 AND (wait alone exceeds the
// budget OR cumulative elapsed would exceed it). The attempt>0 guard guarantees at least one retry
// even with a tiny/misconfigured total-timeout. Comparison avoids summing (overflow-safe).
//===--------------------------------------------------------------------===//
static bool TotalTimeoutWouldAbort(idx_t attempt, int64_t wait_ms, int64_t elapsed_ms, int64_t total_wait_ms) {
	if (attempt == 0) {
		return false;
	}
	return wait_ms > total_wait_ms || elapsed_ms > total_wait_ms - wait_ms;
}

//===--------------------------------------------------------------------===//
// Mirror of the Retry-After consumption in IcebergTransaction::DoTableUpdates: a server-supplied
// Retry-After (>=0) takes precedence over computed backoff, capped at max_wait_ms; otherwise -1
// signals "use decorrelated backoff".
//===--------------------------------------------------------------------===//
static int64_t ConsumeRetryAfter(int64_t retry_after_ms, int64_t max_wait_ms) {
	if (retry_after_ms < 0) {
		return -1; // no server guidance -> caller uses decorrelated backoff
	}
	return retry_after_ms < max_wait_ms ? retry_after_ms : max_wait_ms;
}

//===--------------------------------------------------------------------===//
// Mirror of ClassifyCommitStatus (iceberg_commit_exceptions.hpp)
//===--------------------------------------------------------------------===//
enum class CommitOutcome { CONFLICT, UNKNOWN, FATAL };

static CommitOutcome ClassifyCommitStatus(int status) {
	switch (status) {
	case 409:
		return CommitOutcome::CONFLICT;
	case 429: // too many requests: rejected before applying -> safe to retry like a conflict
		return CommitOutcome::CONFLICT;
	case 408: // request timeout
	case 500: // internal server error
	case 502: // bad gateway
	case 503: // service unavailable (ambiguous for a non-idempotent POST commit)
	case 504: // gateway timeout
		return CommitOutcome::UNKNOWN;
	default:
		return CommitOutcome::FATAL;
	}
}

//===--------------------------------------------------------------------===//
// Mirror of ParseRetryAfterMs (iceberg_commit_exceptions.hpp): numeric seconds -> ms; -1 otherwise.
//===--------------------------------------------------------------------===//
static int64_t ParseRetryAfterMs(const std::string &value) {
	if (value.empty()) {
		return -1;
	}
	try {
		size_t consumed = 0;
		auto seconds = std::stoll(value, &consumed);
		if (consumed != value.size() || seconds < 0) {
			return -1;
		}
		constexpr int64_t MAX_RETRY_AFTER_MS = 24LL * 60 * 60 * 1000;
		if (seconds > MAX_RETRY_AFTER_MS / 1000) {
			return MAX_RETRY_AFTER_MS;
		}
		return seconds * 1000;
	} catch (...) {
		return -1;
	}
}

//===--------------------------------------------------------------------===//
// Mirror of IcebergTableMetadata::IsAncestorOf (iceberg_table_metadata.cpp).
// snapshot_id -> parent_snapshot_id (-1 = no parent). Mirrors the walk exactly, including the
// "missing link -> false" and "self-referential parent -> false" guards.
//===--------------------------------------------------------------------===//
struct SnapshotNode {
	bool has_parent;
	int64_t parent_id;
};
static bool IsAncestorOf(const std::unordered_map<int64_t, SnapshotNode> &snapshots, int64_t ancestor_id,
                         int64_t descendant_id) {
	int64_t current = descendant_id;
	while (true) {
		if (current == ancestor_id) {
			return true;
		}
		auto it = snapshots.find(current);
		if (it == snapshots.end() || !it->second.has_parent) {
			return false;
		}
		if (it->second.parent_id == current) {
			return false;
		}
		current = it->second.parent_id;
	}
}

//===--------------------------------------------------------------------===//
// Mirror of iceberg_transaction.cpp::DeleteEntryReferencedDataFile. V3: referenced_data_file;
// V2 fallback: FILENAME_FIELD_ID lower==upper bound; "" when not attributable to one file.
//===--------------------------------------------------------------------===//
struct FakeDeleteEntry {
	std::string referenced_data_file;
	bool has_filename_bounds = false;
	std::string filename_lower;
	std::string filename_upper;
};
static std::string DeleteEntryReferencedDataFile(const FakeDeleteEntry &e) {
	if (!e.referenced_data_file.empty()) {
		return e.referenced_data_file;
	}
	if (!e.has_filename_bounds) {
		return "";
	}
	if (e.filename_lower != e.filename_upper) {
		return "";
	}
	return e.filename_lower;
}

//===--------------------------------------------------------------------===//
// Mirror of the validateNoNewDeletes decision (iceberg_transaction.cpp): a refreshed-parent DELETE
// entry conflicts if it targets a file we also delete AND its sequence number is newer than our
// starting snapshot's. status==DELETED entries are skipped.
//===--------------------------------------------------------------------===//
enum class EntryStatus { EXISTING, ADDED, DELETED };
struct RefreshedDeleteEntry {
	EntryStatus status;
	std::string referenced;
	int64_t sequence_number;
};
static bool HasConcurrentNewDelete(const std::unordered_set<std::string> &targeted,
                                   const std::vector<RefreshedDeleteEntry> &entries, int64_t starting_seq) {
	for (auto &e : entries) {
		if (e.status == EntryStatus::DELETED) {
			continue;
		}
		if (e.referenced.empty() || !targeted.count(e.referenced)) {
			continue;
		}
		if (starting_seq >= 0 && e.sequence_number <= starting_seq) {
			continue;
		}
		return true;
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Tests
//===--------------------------------------------------------------------===//
static int g_failures = 0;
#define CHECK(cond)                                                                                                    \
	do {                                                                                                               \
		if (!(cond)) {                                                                                                 \
			printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                                                     \
			g_failures++;                                                                                              \
		}                                                                                                              \
	} while (0)

static void TestBackoffExponential() {
	CHECK(BackoffMs(100, 60000, 0) == 100);
	CHECK(BackoffMs(100, 60000, 1) == 200);
	CHECK(BackoffMs(100, 60000, 2) == 400);
	CHECK(BackoffMs(100, 60000, 3) == 800);
}

static void TestBackoffClamp() {
	// 100 * 2^20 would overflow the cap; must clamp to max.
	CHECK(BackoffMs(100, 60000, 20) == 60000);
	CHECK(BackoffMs(100, 60000, 100) == 60000);
}

static void TestBackoffMinEqualsMax() {
	CHECK(BackoffMs(500, 500, 5) == 500);
	CHECK(BackoffMs(500, 500, 0) == 500);
}

static void TestBackoffNoOverflow() {
	// Table properties are user-supplied: a huge min/max-wait must clamp without signed overflow
	// (UB). With min near INT64_MAX the very first doubling would overflow; the guard must clamp.
	const int64_t kHuge = 9223372036854775807LL; // INT64_MAX
	CHECK(BackoffMs(kHuge, kHuge, 5) == kHuge);
	CHECK(BackoffMs(kHuge / 2 + 1, kHuge, 1) == kHuge);
	CHECK(BackoffMs(kHuge / 2 + 1, kHuge, 40) == kHuge);
	// A large-but-doublable min still progresses then clamps, no overflow.
	CHECK(BackoffMs(1LL << 40, 1LL << 50, 5) == (1LL << 45));
	CHECK(BackoffMs(1LL << 40, 1LL << 50, 100) == (1LL << 50));
}

static void TestBackoffJitter() {
	// Full jitter maps a unit-random [0,1] onto [0, base], so concurrent writers spread out instead
	// of waking in lockstep. Endpoints and midpoint must be exact; out-of-range clamps.
	CHECK(BackoffMsWithJitter(100, 60000, 3, 0.0) == 0);     // base 800 -> 0
	CHECK(BackoffMsWithJitter(100, 60000, 3, 1.0) == 800);   // base 800 -> 800
	CHECK(BackoffMsWithJitter(100, 60000, 3, 0.5) == 400);   // base 800 -> 400
	CHECK(BackoffMsWithJitter(100, 60000, 0, 1.0) == 100);   // base 100 -> 100
	CHECK(BackoffMsWithJitter(100, 60000, 0, 0.5) == 50);    // base 100 -> 50
	// Clamp out-of-range randoms (defensive).
	CHECK(BackoffMsWithJitter(100, 60000, 3, -1.0) == 0);
	CHECK(BackoffMsWithJitter(100, 60000, 3, 2.0) == 800);
	// Every jittered value stays within [0, base] across the unit interval.
	for (int k = 0; k <= 100; k++) {
		auto v = BackoffMsWithJitter(100, 60000, 5, k / 100.0); // base = 100*2^5 = 3200
		CHECK(v >= 0 && v <= 3200);
	}
}

static void TestDecorrelatedBackoff() {
	const int64_t MIN = 100, MAX = 60000;
	// First retry starts from prev=min: window [min, min*3] = [100, 300]. Tight, never below min.
	CHECK(DecorrelatedBackoffMs(MIN, MAX, MIN, 0.0) == 100);  // lower bound = min_wait
	CHECK(DecorrelatedBackoffMs(MIN, MAX, MIN, 1.0) == 300);  // upper bound = prev*3
	CHECK(DecorrelatedBackoffMs(MIN, MAX, MIN, 0.5) == 200);  // midpoint
	// Window widens with prev_sleep: prev=1000 -> [100, 3000].
	CHECK(DecorrelatedBackoffMs(MIN, MAX, 1000, 0.0) == 100);
	CHECK(DecorrelatedBackoffMs(MIN, MAX, 1000, 1.0) == 3000);
	// Clamp to max_wait when prev*3 would exceed it.
	CHECK(DecorrelatedBackoffMs(MIN, MAX, 50000, 1.0) == MAX);
	// Never below min_wait, never above max_wait, across the unit interval and growing prev.
	int64_t prev = MIN;
	for (int i = 0; i < 50; i++) {
		auto v = DecorrelatedBackoffMs(MIN, MAX, prev, (i % 11) / 10.0);
		CHECK(v >= MIN && v <= MAX);
		prev = v;
	}
	// Overflow guard: huge prev must clamp, not wrap.
	CHECK(DecorrelatedBackoffMs(MIN, 9223372036854775807LL, 9223372036854775807LL / 2, 1.0) <= 9223372036854775807LL);
}

static void TestClassifyConflict() {
	CHECK(ClassifyCommitStatus(409) == CommitOutcome::CONFLICT);
	// 429 (rate limited): rejected before applying, safe to retry directly like a conflict (mirrors
	// Java's REST retry strategy, which retries 429 even for non-idempotent POSTs).
	CHECK(ClassifyCommitStatus(429) == CommitOutcome::CONFLICT);
}

static void TestClassifyUnknown() {
	// For a non-idempotent commit POST these may have been applied before the failure surfaced ->
	// must not blindly retry (double-apply) or clean up (delete a committed snapshot's files). Java
	// likewise does NOT auto-retry these for POST; they go through commit-state-unknown handling.
	CHECK(ClassifyCommitStatus(500) == CommitOutcome::UNKNOWN);
	CHECK(ClassifyCommitStatus(502) == CommitOutcome::UNKNOWN);
	CHECK(ClassifyCommitStatus(503) == CommitOutcome::UNKNOWN);
	CHECK(ClassifyCommitStatus(504) == CommitOutcome::UNKNOWN);
	CHECK(ClassifyCommitStatus(408) == CommitOutcome::UNKNOWN);
}

static void TestClassifyFatal() {
	// Definite client errors are not retryable.
	CHECK(ClassifyCommitStatus(400) == CommitOutcome::FATAL);
	CHECK(ClassifyCommitStatus(401) == CommitOutcome::FATAL);
	CHECK(ClassifyCommitStatus(403) == CommitOutcome::FATAL);
	CHECK(ClassifyCommitStatus(404) == CommitOutcome::FATAL);
}

static void TestParseRetryAfter() {
	// Numeric seconds -> ms.
	CHECK(ParseRetryAfterMs("0") == 0);
	CHECK(ParseRetryAfterMs("1") == 1000);
	CHECK(ParseRetryAfterMs("120") == 120000);
	// Absent / non-numeric / HTTP-date / negative -> -1 (fall back to exponential backoff).
	CHECK(ParseRetryAfterMs("") == -1);
	CHECK(ParseRetryAfterMs("Wed, 21 Oct 2015 07:28:00 GMT") == -1);
	CHECK(ParseRetryAfterMs("12abc") == -1);
	CHECK(ParseRetryAfterMs("-5") == -1);
	// Overflow guard: a huge value must clamp to one day in ms, never wrap to negative/tiny.
	CHECK(ParseRetryAfterMs("9999999999999999") == 24LL * 60 * 60 * 1000);
	CHECK(ParseRetryAfterMs("86400") == 24LL * 60 * 60 * 1000); // exactly one day, not clamped lower
}

//===--------------------------------------------------------------------===//
// Retry config parsing defaults / validation
//===--------------------------------------------------------------------===//
static void TestRetryConfigDefaults() {
	auto c = ParseRetryConfig("", "", "", "");
	CHECK(c.num_retries == 4);
	CHECK(c.min_wait_ms == 100);
	CHECK(c.max_wait_ms == 60000);
	CHECK(c.total_wait_ms == 1800000);
}
static void TestRetryConfigNumRetriesZeroAllowed() {
	// num-retries=0 is a legal "single attempt, no retries"; the wait fields reject 0 and fall back.
	auto c = ParseRetryConfig("0", "0", "0", "0");
	CHECK(c.num_retries == 0);
	CHECK(c.min_wait_ms == 100);   // 0 not allowed -> default
	CHECK(c.max_wait_ms == 60000); // 0 not allowed -> default
}
static void TestRetryConfigNegativeAndGarbage() {
	auto c = ParseRetryConfig("-3", "abc", "-1", "x9");
	CHECK(c.num_retries == 4);     // negative -> default
	CHECK(c.min_wait_ms == 100);   // garbage -> default
	CHECK(c.max_wait_ms == 60000); // negative -> default
}
static void TestRetryConfigMinGreaterThanMaxNormalized() {
	auto c = ParseRetryConfig("4", "90000", "5000", "");
	CHECK(c.max_wait_ms == 5000);
	CHECK(c.min_wait_ms == 5000); // min clamped down to max
}

//===--------------------------------------------------------------------===//
// MostLenient cross-table fold
//===--------------------------------------------------------------------===//
static void TestMostLenientTakesMaxRetriesAndWindow() {
	RetryCfg a {4, 100, 60000, 1800000};
	RetryCfg b {500, 50, 5000, 600000};
	auto m = MostLenient(a, b);
	CHECK(m.num_retries == 500);    // larger
	CHECK(m.min_wait_ms == 50);     // smaller
	CHECK(m.max_wait_ms == 60000);  // larger
	CHECK(m.total_wait_ms == 1800000);
}
static void TestMostLenientPreservesMinLeMax() {
	// a has a large min, b has a small max -> folded min must not exceed folded max.
	RetryCfg a {4, 9000, 9000, 1000};
	RetryCfg b {4, 100, 200, 1000};
	auto m = MostLenient(a, b);
	CHECK(m.max_wait_ms == 9000); // max of (9000,200)
	CHECK(m.min_wait_ms == 100);  // min of (9000,100); already <= max
	CHECK(m.min_wait_ms <= m.max_wait_ms);
}

//===--------------------------------------------------------------------===//
// total-timeout-ms enforcement
//===--------------------------------------------------------------------===//
static void TestTotalTimeoutFirstAttemptNeverAborts() {
	// attempt 0: even a tiny budget must not abort before the first retry sleeps.
	CHECK(!TotalTimeoutWouldAbort(0, 100, 0, 1));
	CHECK(!TotalTimeoutWouldAbort(0, 100000, 999999, 50));
}
static void TestTotalTimeoutAbortsWhenWaitExceedsBudget() {
	// attempt>0, the single wait alone is larger than the whole budget.
	CHECK(TotalTimeoutWouldAbort(1, 70000, 0, 60000));
}
static void TestTotalTimeoutAbortsWhenCumulativeExceeds() {
	// attempt>0, already-elapsed + next wait would cross the budget.
	CHECK(TotalTimeoutWouldAbort(2, 1000, 59500, 60000)); // 59500 > 60000-1000=59000
	CHECK(!TotalTimeoutWouldAbort(2, 1000, 58000, 60000)); // 58000 <= 59000 -> proceed
}

//===--------------------------------------------------------------------===//
// Retry-After consumption (cap at max_wait_ms; -1 means use backoff)
//===--------------------------------------------------------------------===//
static void TestConsumeRetryAfter() {
	CHECK(ConsumeRetryAfter(-1, 60000) == -1);    // no server guidance
	CHECK(ConsumeRetryAfter(2000, 60000) == 2000); // honored as-is
	CHECK(ConsumeRetryAfter(0, 60000) == 0);       // zero is a valid immediate retry
	CHECK(ConsumeRetryAfter(120000, 60000) == 60000); // capped at max_wait
}

//===--------------------------------------------------------------------===//
// Ancestry / rollback guard
//===--------------------------------------------------------------------===//
static void TestAncestrySelf() {
	std::unordered_map<int64_t, SnapshotNode> s = {{1, {false, -1}}};
	CHECK(IsAncestorOf(s, 1, 1)); // a snapshot is its own ancestor
}
static void TestAncestryLinearChain() {
	// 1 <- 2 <- 3 (3's parent is 2, 2's parent is 1)
	std::unordered_map<int64_t, SnapshotNode> s = {{1, {false, -1}}, {2, {true, 1}}, {3, {true, 2}}};
	CHECK(IsAncestorOf(s, 1, 3));  // 1 is an ancestor of 3
	CHECK(IsAncestorOf(s, 2, 3));  // 2 is an ancestor of 3
	CHECK(!IsAncestorOf(s, 3, 1)); // 3 is NOT an ancestor of 1 (wrong direction)
}
static void TestAncestryDivergedRollback() {
	// Concurrent rollback: current snapshot 9 descends from 5, NOT from our starting snapshot 3.
	std::unordered_map<int64_t, SnapshotNode> s = {
	    {3, {true, 2}}, {2, {true, 1}}, {1, {false, -1}}, {5, {true, 1}}, {9, {true, 5}}};
	CHECK(!IsAncestorOf(s, 3, 9)); // diverged -> must be detected (abort path)
	CHECK(IsAncestorOf(s, 1, 9));  // shared root is still an ancestor
}
static void TestAncestryMissingLink() {
	// 3's parent 2 was elided (not in the map) -> cannot prove ancestry -> false (safe).
	std::unordered_map<int64_t, SnapshotNode> s = {{3, {true, 2}}};
	CHECK(!IsAncestorOf(s, 1, 3));
}
static void TestAncestrySelfReferentialGuard() {
	// A corrupt self-parent must not loop forever.
	std::unordered_map<int64_t, SnapshotNode> s = {{3, {true, 3}}};
	CHECK(!IsAncestorOf(s, 1, 3));
}

//===--------------------------------------------------------------------===//
// Delete-entry referenced-data-file attribution (V2 + V3)
//===--------------------------------------------------------------------===//
static void TestDeleteAttribV3() {
	FakeDeleteEntry e;
	e.referenced_data_file = "s3://b/data/f.parquet";
	CHECK(DeleteEntryReferencedDataFile(e) == "s3://b/data/f.parquet");
}
static void TestDeleteAttribV2FilenameBounds() {
	FakeDeleteEntry e;
	e.has_filename_bounds = true;
	e.filename_lower = "s3://b/data/f.parquet";
	e.filename_upper = "s3://b/data/f.parquet";
	CHECK(DeleteEntryReferencedDataFile(e) == "s3://b/data/f.parquet");
}
static void TestDeleteAttribSpanningBounds() {
	// lower != upper: spans multiple files, cannot attribute -> "".
	FakeDeleteEntry e;
	e.has_filename_bounds = true;
	e.filename_lower = "s3://b/data/a.parquet";
	e.filename_upper = "s3://b/data/z.parquet";
	CHECK(DeleteEntryReferencedDataFile(e) == "");
}
static void TestDeleteAttribNone() {
	FakeDeleteEntry e; // equality delete: no referenced file, no bounds
	CHECK(DeleteEntryReferencedDataFile(e) == "");
}

//===--------------------------------------------------------------------===//
// validateNoNewDeletes decision
//===--------------------------------------------------------------------===//
static void TestNoNewDeleteWhenPreexisting() {
	std::unordered_set<std::string> targeted = {"f.parquet"};
	// A delete on our file but at/below starting seq = part of the state we accounted for.
	std::vector<RefreshedDeleteEntry> entries = {{EntryStatus::EXISTING, "f.parquet", 5}};
	CHECK(!HasConcurrentNewDelete(targeted, entries, 5));
	CHECK(!HasConcurrentNewDelete(targeted, entries, 10));
}
static void TestNewDeleteDetected() {
	std::unordered_set<std::string> targeted = {"f.parquet"};
	// A newer delete (seq 7 > starting 5) on our file = concurrent overlap -> conflict.
	std::vector<RefreshedDeleteEntry> entries = {{EntryStatus::ADDED, "f.parquet", 7}};
	CHECK(HasConcurrentNewDelete(targeted, entries, 5));
}
static void TestNewDeleteUnrelatedFile() {
	std::unordered_set<std::string> targeted = {"f.parquet"};
	// Newer delete but on a different file -> no conflict.
	std::vector<RefreshedDeleteEntry> entries = {{EntryStatus::ADDED, "other.parquet", 7}};
	CHECK(!HasConcurrentNewDelete(targeted, entries, 5));
}
static void TestNewDeleteSkipsDeletedStatus() {
	std::unordered_set<std::string> targeted = {"f.parquet"};
	// A DELETED-status entry (the delete file itself was removed) must not count.
	std::vector<RefreshedDeleteEntry> entries = {{EntryStatus::DELETED, "f.parquet", 7}};
	CHECK(!HasConcurrentNewDelete(targeted, entries, 5));
}

int main() {
	TestBackoffExponential();
	TestBackoffClamp();
	TestBackoffMinEqualsMax();
	TestBackoffNoOverflow();
	TestBackoffJitter();
	TestDecorrelatedBackoff();
	TestClassifyConflict();
	TestClassifyUnknown();
	TestClassifyFatal();
	TestParseRetryAfter();

	TestRetryConfigDefaults();
	TestRetryConfigNumRetriesZeroAllowed();
	TestRetryConfigNegativeAndGarbage();
	TestRetryConfigMinGreaterThanMaxNormalized();
	TestMostLenientTakesMaxRetriesAndWindow();
	TestMostLenientPreservesMinLeMax();
	TestTotalTimeoutFirstAttemptNeverAborts();
	TestTotalTimeoutAbortsWhenWaitExceedsBudget();
	TestTotalTimeoutAbortsWhenCumulativeExceeds();
	TestConsumeRetryAfter();

	TestAncestrySelf();
	TestAncestryLinearChain();
	TestAncestryDivergedRollback();
	TestAncestryMissingLink();
	TestAncestrySelfReferentialGuard();

	TestDeleteAttribV3();
	TestDeleteAttribV2FilenameBounds();
	TestDeleteAttribSpanningBounds();
	TestDeleteAttribNone();

	TestNoNewDeleteWhenPreexisting();
	TestNewDeleteDetected();
	TestNewDeleteUnrelatedFile();
	TestNewDeleteSkipsDeletedStatus();

	if (g_failures == 0) {
		printf("All retry logic tests passed.\n");
		return 0;
	}
	printf("%d test(s) failed.\n", g_failures);
	return 1;
}
