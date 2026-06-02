#pragma once

#include "duckdb/common/types/timestamp.hpp"
#include "function/ducklake/ducklake_metadata_serializer.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

struct DuckLakeSnapshot {
public:
	DuckLakeSnapshot(timestamp_t timestamp);

public:
	string FinalizeEntry(DuckLakeMetadataSerializer &serializer);

public:
	int64_t AddSchema(const string &schema_name);
	void DropSchema(const string &schema_name);
	int64_t AddTable(const string &table_uuid);
	void DropTable(const string &table_uuid);
	int64_t AddDataFile(const string &table_uuid);
	void DeleteDataFile(const string &table_uuid);
	int64_t AddDeleteFile(const string &table_uuid);
	void AlterTable(const string &table_uuid);

public:
	//! The snapshot id is assigned after we've processed all tables
	optional_idx snapshot_id;
	timestamp_t snapshot_time;

public:
	//! table_uuid set
	unordered_set<string> created_table;
	unordered_set<string> inserted_into_table;
	unordered_set<string> deleted_from_table;
	unordered_set<string> dropped_table;
	unordered_set<string> altered_table;

	//! schema name set
	unordered_set<string> created_schema;
	unordered_set<string> dropped_schema;

public:
	//! schemas/tables/views changed
	idx_t catalog_changes = 0;
	//! schemas/tables/views added
	idx_t catalog_additions = 0;
	//! data or delete files added
	idx_t files_added = 0;

	//! These are populated after FinalizeEntry has been called
	int64_t base_schema_version;
	int64_t base_catalog_id;
	int64_t base_file_id;
};

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
