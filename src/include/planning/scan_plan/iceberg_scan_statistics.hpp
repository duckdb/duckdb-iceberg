#pragma once

#include "planning/metadata_io/manifest_list/bound_iceberg_manifest_list_entry.hpp"

namespace duckdb {

//! Count policy for a pruned manifest view. Does not load manifests or advance scan cursors.
//! Estimates may account for delete counts; exact counts require no matching delete manifests.
class IcebergScanStatistics {
public:
	IcebergScanStatistics(const vector<BoundIcebergManifestListEntry> &data_manifests,
	                      const vector<bool> &data_manifest_matches,
	                      const vector<BoundIcebergManifestListEntry> &delete_manifests,
	                      const vector<bool> &delete_manifest_matches);

	optional<idx_t> EstimateCardinality() const;
	optional<idx_t> ExactRowCount() const;

private:
	optional<idx_t> CountDataRows() const;

	const vector<BoundIcebergManifestListEntry> &data_manifests;
	const vector<bool> &data_manifest_matches;
	const vector<BoundIcebergManifestListEntry> &delete_manifests;
	const vector<bool> &delete_manifest_matches;
};

} // namespace duckdb
