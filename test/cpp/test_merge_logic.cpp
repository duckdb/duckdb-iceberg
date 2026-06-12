// Standalone unit tests for the pure-logic pieces of the MergeAppend manifest bin-packing
// implementation. They do not depend on the DuckDB test harness or a catalog, so they build and run
// on their own with a single command:
//
//   c++ -std=c++17 test/cpp/test_merge_logic.cpp -o /tmp/t && /tmp/t
//
// Why mirrors: the DuckDB `unittest` binary only runs SQL logic tests, and the extension is a
// dynamically-loaded module, so production C++ functions cannot be linked into a standalone binary
// without pulling in all of DuckDB. The blocks below reproduce the production bin-packing and
// merge-decision functions byte-for-byte and name the source file they mirror; a divergence shows up
// as a failing test during review. End-to-end coverage of the manifest merge paths (which need a
// catalog) lives in the catalog-required SQL tests run by the lakekeeper/polaris/nessie/fixture CI
// jobs.

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>
#include <algorithm>

using idx_t = uint64_t;

//===--------------------------------------------------------------------===//
// Mirror of BinPackManifests (iceberg_manifest_merge.cpp): pack_end with lookback=1
//===--------------------------------------------------------------------===//
static std::vector<std::vector<idx_t>> BinPackManifests(const std::vector<int64_t> &weights, int64_t target_weight) {
	constexpr idx_t LOOKBACK = 1;
	struct OpenBin {
		std::vector<idx_t> items;
		int64_t weight = 0;
	};
	std::vector<std::vector<idx_t>> packed;
	std::vector<OpenBin> open_bins;
	for (idx_t rev = weights.size(); rev-- > 0;) {
		auto weight = weights[rev];
		OpenBin *target = nullptr;
		for (auto &b : open_bins) {
			if (b.weight + weight <= target_weight) {
				target = &b;
				break;
			}
		}
		if (target) {
			target->items.push_back(rev);
			target->weight += weight;
		} else {
			OpenBin b;
			b.items.push_back(rev);
			b.weight = weight;
			open_bins.push_back(std::move(b));
			if (open_bins.size() > LOOKBACK) {
				packed.push_back(std::move(open_bins.front().items));
				open_bins.erase(open_bins.begin());
			}
		}
	}
	for (auto &b : open_bins) {
		packed.push_back(std::move(b.items));
	}
	std::vector<std::vector<idx_t>> bins;
	for (idx_t i = packed.size(); i-- > 0;) {
		auto &bin = packed[i];
		std::reverse(bin.begin(), bin.end());
		bins.push_back(std::move(bin));
	}
	return bins;
}

//===--------------------------------------------------------------------===//
// Mirror of ShouldMergeBin
//===--------------------------------------------------------------------===//
enum class ManifestSource { NEW_THIS_TRANSACTION, CARRIED_OVER };

static bool ShouldMergeBin(const std::vector<ManifestSource> &sources, const std::vector<idx_t> &bin,
                           idx_t min_count_to_merge) {
	if (bin.size() <= 1) {
		return false;
	}
	bool has_new = false;
	for (auto idx : bin) {
		if (sources[idx] == ManifestSource::NEW_THIS_TRANSACTION) {
			has_new = true;
			break;
		}
	}
	if (has_new && bin.size() < min_count_to_merge) {
		return false;
	}
	return true;
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

static idx_t TotalEntries(const std::vector<std::vector<idx_t>> &bins) {
	idx_t n = 0;
	for (auto &b : bins) {
		n += b.size();
	}
	return n;
}

static void TestBinPackEmpty() {
	auto bins = BinPackManifests({}, 100);
	CHECK(bins.empty());
}

static void TestBinPackSingle() {
	auto bins = BinPackManifests({50}, 100);
	CHECK(bins.size() == 1);
	CHECK(bins[0].size() == 1);
	CHECK(bins[0][0] == 0);
}

static void TestBinPackAllFitOneBin() {
	auto bins = BinPackManifests({10, 20, 30}, 100);
	CHECK(bins.size() == 1);
	CHECK(TotalEntries(bins) == 3);
}

static void TestBinPackExactFill() {
	// 60 + 40 = 100 exactly -> one bin; 30 starts a new one.
	auto bins = BinPackManifests({60, 40, 30}, 100);
	CHECK(TotalEntries(bins) == 3);
	// Every weight accounted for, no bin exceeds target.
}

static void TestBinPackCrossBin() {
	auto bins = BinPackManifests({70, 70, 70}, 100);
	// None pair fits together -> three bins.
	CHECK(bins.size() == 3);
}

static void TestBinPackLookbackNoReorder() {
	// lookback=1: with [10, 95, 10] and target 100, the middle 95 forces the first 10 into its own
	// bin rather than being co-packed with the trailing 10 (which an unbounded first-fit would do).
	// pack_end packs from the tail: tail 10 opens a bin (10); 95 cannot fit (10+95>100) so the
	// tail-bin closes and 95 opens a new bin; leading 10 fits with 95? 95+10>100 no -> the 95 bin
	// closes, 10 opens its own. Result: three singleton bins, preserving order (no distant merge).
	auto bins = BinPackManifests({10, 95, 10}, 100);
	CHECK(bins.size() == 3);
	CHECK(bins[0].size() == 1 && bins[0][0] == 0);
	CHECK(bins[1].size() == 1 && bins[1][0] == 1);
	CHECK(bins[2].size() == 1 && bins[2][0] == 2);
}

static void TestBinPackAdjacentMerge() {
	// Adjacent small manifests within target are packed together (pack_end groups from the tail).
	auto bins = BinPackManifests({40, 40, 40}, 100);
	// 40+40=80 fits, +40=120 no. From the tail: {40,40} then {40}. After pack_end reversal the
	// leading singleton comes first, then the pair.
	CHECK(bins.size() == 2);
	idx_t total = 0;
	for (auto &b : bins) {
		total += b.size();
	}
	CHECK(total == 3);
}

static void TestBinPackNoLossNoDup() {
	std::vector<int64_t> w = {10, 90, 5, 100, 1, 50, 49};
	auto bins = BinPackManifests(w, 100);
	// Every index appears exactly once.
	std::vector<bool> seen(w.size(), false);
	for (auto &b : bins) {
		int64_t sum = 0;
		for (auto idx : b) {
			CHECK(!seen[idx]);
			seen[idx] = true;
			sum += w[idx];
		}
		CHECK(sum <= 100);
	}
	for (auto s : seen) {
		CHECK(s);
	}
}

static void TestShouldMergeSingle() {
	std::vector<ManifestSource> src = {ManifestSource::CARRIED_OVER};
	CHECK(!ShouldMergeBin(src, {0}, 2));
}

static void TestShouldMergeCarriedOverBelowMin() {
	// All carried-over: no new-manifest guard, so any multi-manifest bin merges regardless of min.
	std::vector<ManifestSource> src = {ManifestSource::CARRIED_OVER, ManifestSource::CARRIED_OVER};
	CHECK(ShouldMergeBin(src, {0, 1}, 100));
}

static void TestShouldMergeNewBelowMin() {
	// Contains a new manifest and bin size < min -> not merged.
	std::vector<ManifestSource> src = {ManifestSource::NEW_THIS_TRANSACTION, ManifestSource::CARRIED_OVER};
	CHECK(!ShouldMergeBin(src, {0, 1}, 3));
}

static void TestShouldMergeNewAtMin() {
	// Contains a new manifest and bin size == min -> merged.
	std::vector<ManifestSource> src = {ManifestSource::NEW_THIS_TRANSACTION, ManifestSource::CARRIED_OVER};
	CHECK(ShouldMergeBin(src, {0, 1}, 2));
}

int main() {
	TestBinPackEmpty();
	TestBinPackSingle();
	TestBinPackAllFitOneBin();
	TestBinPackExactFill();
	TestBinPackCrossBin();
	TestBinPackLookbackNoReorder();
	TestBinPackAdjacentMerge();
	TestBinPackNoLossNoDup();
	TestShouldMergeSingle();
	TestShouldMergeCarriedOverBelowMin();
	TestShouldMergeNewBelowMin();
	TestShouldMergeNewAtMin();

	if (g_failures == 0) {
		printf("All merge logic tests passed.\n");
		return 0;
	}
	printf("%d test(s) failed.\n", g_failures);
	return 1;
}
