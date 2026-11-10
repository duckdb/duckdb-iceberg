#include "planning/scan_plan/iceberg_scan_statistics.hpp"

namespace duckdb {

IcebergScanStatistics::IcebergScanStatistics(const vector<BoundIcebergManifestListEntry> &data_manifests_p,
                                             const vector<bool> &data_manifest_matches_p,
                                             const vector<BoundIcebergManifestListEntry> &delete_manifests_p,
                                             const vector<bool> &delete_manifest_matches_p)
    : data_manifests(data_manifests_p), data_manifest_matches(data_manifest_matches_p),
      delete_manifests(delete_manifests_p), delete_manifest_matches(delete_manifest_matches_p) {
	D_ASSERT(data_manifests.size() == data_manifest_matches.size());
	D_ASSERT(delete_manifests.size() == delete_manifest_matches.size());
}

optional<idx_t> IcebergScanStatistics::CountDataRows() const {
	idx_t count = 0;
	for (idx_t i = 0; i < data_manifests.size(); i++) {
		if (!data_manifest_matches[i]) {
			continue;
		}
		auto &counts = data_manifests[i].entry.GetManifest().counts;
		if (!counts || !counts->added_rows_count || !counts->existing_rows_count) {
			return nullopt;
		}
		//! The counts come from the manifest list: if they are implausible the cardinality is unknown
		static constexpr idx_t MAX_ROW_COUNT = NumericLimits<int64_t>::Maximum();
		auto added_rows = *counts->added_rows_count;
		auto existing_rows = *counts->existing_rows_count;
		if (added_rows > MAX_ROW_COUNT || existing_rows > MAX_ROW_COUNT || count > MAX_ROW_COUNT) {
			return nullopt;
		}
		count += added_rows;
		if (count > MAX_ROW_COUNT) {
			return nullopt;
		}
		count += existing_rows;
		if (count > MAX_ROW_COUNT) {
			return nullopt;
		}
	}
	return count;
}

optional<idx_t> IcebergScanStatistics::EstimateCardinality() const {
	auto cardinality = CountDataRows();
	if (!cardinality) {
		return nullopt;
	}
	for (idx_t i = 0; i < delete_manifests.size(); i++) {
		if (!delete_manifest_matches[i]) {
			continue;
		}
		auto &counts = delete_manifests[i].entry.GetManifest().counts;
		if (!counts || !counts->added_rows_count || !counts->existing_rows_count) {
			return nullopt;
		}
		// Both added and existing delete files are live. Deletes can overlap, so clamp the estimate at zero.
		*cardinality -= MinValue(*cardinality, *counts->added_rows_count);
		*cardinality -= MinValue(*cardinality, *counts->existing_rows_count);
	}
	return cardinality;
}

optional<idx_t> IcebergScanStatistics::ExactRowCount() const {
	for (auto matches : delete_manifest_matches) {
		if (matches) {
			return nullopt;
		}
	}
	return CountDataRows();
}

} // namespace duckdb
