#include "function/ducklake/ducklake_snapshot.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

DuckLakeSnapshot::DuckLakeSnapshot(timestamp_t timestamp) : snapshot_time(timestamp) {
}

string DuckLakeSnapshot::FinalizeEntry(DuckLakeMetadataSerializer &serializer) {
	//! Set these, so we can use these to create the correct ids for the catalog/file entries added by this snapshot
	base_schema_version = serializer.schema_version;
	base_catalog_id = serializer.next_catalog_id;
	base_file_id = serializer.next_file_id;

	//! Update the serializer to point to the next id starts
	serializer.schema_version += !!catalog_changes;
	serializer.next_catalog_id += catalog_additions;
	serializer.next_file_id += files_added;

	int64_t snapshot_id = serializer.snapshot_id++;
	this->snapshot_id = snapshot_id;

	int64_t schema_version = serializer.schema_version;
	int64_t next_catalog_id = serializer.next_catalog_id;
	int64_t next_file_id = serializer.next_file_id;
	return StringUtil::Format("VALUES(%d, '%s', %d, %d, %d);", snapshot_id, Timestamp::ToString(snapshot_time),
	                          schema_version, next_catalog_id, next_file_id);
}

int64_t DuckLakeSnapshot::AddSchema(const string &schema_name) {
	created_schema.insert(schema_name);
	catalog_changes++;
	return catalog_additions++;
}

void DuckLakeSnapshot::DropSchema(const string &schema_name) {
	catalog_changes++;
	dropped_schema.insert(schema_name);
}

int64_t DuckLakeSnapshot::AddTable(const string &table_uuid) {
	created_table.insert(table_uuid);
	catalog_changes++;
	return catalog_additions++;
}

void DuckLakeSnapshot::DropTable(const string &table_uuid) {
	catalog_changes++;
	dropped_table.insert(table_uuid);
}

int64_t DuckLakeSnapshot::AddDataFile(const string &table_uuid) {
	inserted_into_table.insert(table_uuid);
	return files_added++;
}

void DuckLakeSnapshot::DeleteDataFile(const string &table_uuid) {
	deleted_from_table.insert(table_uuid);
}

int64_t DuckLakeSnapshot::AddDeleteFile(const string &table_uuid) {
	deleted_from_table.insert(table_uuid);
	return files_added++;
}

void DuckLakeSnapshot::AlterTable(const string &table_uuid) {
	catalog_changes++;
	altered_table.insert(table_uuid);
}

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
