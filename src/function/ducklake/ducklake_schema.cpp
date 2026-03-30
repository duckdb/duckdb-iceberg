#include "function/ducklake/ducklake_schema.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

DuckLakeSchema::DuckLakeSchema(const string &schema_name) : schema_name(schema_name) {
	//! FIXME: this is generated for now (Iceberg doesn't have "namespace" uuids)
	schema_uuid = UUID::ToString(UUID::GenerateRandomUUID());
}

string DuckLakeSchema::FinalizeEntry(const map<timestamp_t, DuckLakeSnapshot> &snapshots) {
	auto &snapshot = snapshots.at(start_snapshot);
	int64_t schema_id = snapshot.base_catalog_id + catalog_id_offset;
	this->schema_id = schema_id;

	int64_t begin_snapshot = snapshot.snapshot_id.GetIndex();
	return StringUtil::Format("VALUES (%d, '%s', %d, NULL, '%s', '', false);", schema_id, schema_uuid, begin_snapshot,
	                          schema_name);
}

void DuckLakeSchema::AssignEarliestSnapshot(unordered_map<string, DuckLakeTable> &all_tables,
                                            map<timestamp_t, DuckLakeSnapshot> &snapshots) {
	D_ASSERT(!tables.empty());
	timestamp_t snapshot_time;
	for (idx_t i = 0; i < tables.size(); i++) {
		auto &table = all_tables.at(tables[i]);
		auto &table_snapshot = table.all_columns.front().start_snapshot;
		if (!i || snapshot_time > table_snapshot) {
			snapshot_time = table_snapshot;
		}
	}
	start_snapshot = snapshot_time;
	auto &snapshot = snapshots.at(snapshot_time);
	catalog_id_offset = snapshot.AddSchema(schema_name);
}

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
