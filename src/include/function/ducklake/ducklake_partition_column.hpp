#pragma once

#include "duckdb/common/constants.hpp"
#include "core/metadata/partition/iceberg_partition_spec.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

struct DuckLakePartitionColumn {
public:
	DuckLakePartitionColumn(const IcebergPartitionSpecField &field);

public:
	string FinalizeEntry(int64_t table_id, int64_t partition_id, int64_t partition_key_index);

public:
	int64_t column_id;
	string transform;

	//! The iceberg partition field id that this column is mirroring
	uint64_t partition_field_id;
};

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
