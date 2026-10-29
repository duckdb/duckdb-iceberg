#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "planning/metadata_io/manifest/bound_iceberg_manifest_entry.hpp"
#include "planning/metadata_io/manifest_list/bound_iceberg_manifest_list_entry.hpp"

namespace duckdb {

class ClientContext;
struct IcebergTableMetadata;
class IcebergTableSchema;

//! Statistics derived lazily by column and cached for a pinned, statically-pruned scan-planner view.
class IcebergScanStatisticsCache {
public:
	IcebergScanStatisticsCache(ClientContext &context, const IcebergTableMetadata &metadata,
	                           const IcebergTableSchema &schema,
	                           const vector<BoundIcebergManifestListEntry> &data_manifests,
	                           const vector<BoundIcebergManifestEntry> &data_files, bool has_pending_writes);
	~IcebergScanStatisticsCache();

	//! The summed record count of the selected data files, or nullopt when any count is unusable
	optional<idx_t> Cardinality() const;
	unique_ptr<BaseStatistics> GetColumnStatistics(column_t column_index) const;
	void GetPartitionStatistics(vector<PartitionStatistics> &result) const;

private:
	struct State;
	unique_ptr<State> state;
};

} // namespace duckdb
