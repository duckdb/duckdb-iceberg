#include "function/ducklake/ducklake_partition.hpp"
#include "function/ducklake/ducklake_utils.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

DuckLakePartition::DuckLakePartition(const IcebergPartitionSpec &partition) {
	for (auto &field : partition.fields) {
		columns.push_back(DuckLakePartitionColumn(field));
	}
}

string DuckLakePartition::FinalizeEntry(int64_t table_id, DuckLakeMetadataSerializer &serializer,
                                        const map<timestamp_t, DuckLakeSnapshot> &snapshots) {
	auto partition_id = serializer.partition_id++;
	this->partition_id = partition_id;
	auto snapshot_ids = DuckLakeUtils::GetSnapshots(start_snapshot, has_end, end_snapshot, snapshots);

	return StringUtil::Format("VALUES(%d, %d, %d, %s);", partition_id, table_id, snapshot_ids.first,
	                          snapshot_ids.second);
}

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
