#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

struct DuckLakeMetadataSerializer {
public:
	DuckLakeMetadataSerializer() {
	}

public:
	int64_t snapshot_id = 0;
	int64_t partition_id = 0;

	//! Version of the schema of the catalog (this covers the schema, table and even columns of the table)
	int64_t schema_version = 0;
	//! Ids assigned to table_id, schema_id and view_id
	int64_t next_catalog_id = 0;
	//! Ids assigned to data_file_id or delete_file_id
	int64_t next_file_id = 0;
};

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
