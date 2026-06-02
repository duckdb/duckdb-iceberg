#pragma once

#include "duckdb/common/constants.hpp"
#include "function/ducklake/ducklake_table.hpp"
#include "function/ducklake/ducklake_snapshot.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

struct DuckLakeSchema {
public:
	DuckLakeSchema(const string &schema_name);

public:
	string FinalizeEntry(const map<timestamp_t, DuckLakeSnapshot> &snapshots);
	void AssignEarliestSnapshot(unordered_map<string, DuckLakeTable> &all_tables,
	                            map<timestamp_t, DuckLakeSnapshot> &snapshots);

public:
	string schema_name;
	string schema_uuid;

	//! The schema id is assigned after we've processed all tables
	optional_idx schema_id;
	idx_t catalog_id_offset = DConstants::INVALID_INDEX;

	timestamp_t start_snapshot;
	//! List of table uuids that belong to this schema
	vector<string> tables;
};

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
