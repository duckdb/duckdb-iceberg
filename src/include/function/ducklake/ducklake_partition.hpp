#pragma once

#include "duckdb/common/constants.hpp"
#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "function/ducklake/ducklake_snapshot.hpp"
#include "function/ducklake/ducklake_metadata_serializer.hpp"
#include "function/ducklake/ducklake_partition_column.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

struct DuckLakePartition {
public:
	DuckLakePartition(const IcebergPartitionSpec &partition);

public:
	string FinalizeEntry(int64_t table_id, DuckLakeMetadataSerializer &serializer,
	                     const map<timestamp_t, DuckLakeSnapshot> &snapshots);

public:
	//! The id is assigned after we've processed all tables
	optional_idx partition_id;
	vector<DuckLakePartitionColumn> columns;

	timestamp_t start_snapshot;
	bool has_end = false;
	timestamp_t end_snapshot;
};

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
