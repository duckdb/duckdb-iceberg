#include "function/ducklake/ducklake_partition_column.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

DuckLakePartitionColumn::DuckLakePartitionColumn(const IcebergPartitionSpecField &field) {
	switch (field.transform.Type()) {
	case IcebergTransformType::IDENTITY:
	case IcebergTransformType::YEAR:
	case IcebergTransformType::MONTH:
	case IcebergTransformType::DAY:
	case IcebergTransformType::HOUR: {
		transform = field.transform.RawType();
		break;
	}
	case IcebergTransformType::BUCKET:
	case IcebergTransformType::TRUNCATE:
	case IcebergTransformType::VOID:
	default:
		throw InvalidInputException("This type of transform (%s) can not be translated to DuckLake",
		                            field.transform.RawType());
	};
	column_id = field.source_id;
	partition_field_id = field.partition_field_id;
}

string DuckLakePartitionColumn::FinalizeEntry(int64_t table_id, int64_t partition_id, int64_t partition_key_index) {
	return StringUtil::Format("VALUES(%d, %d, %d, %d, '%s');", partition_id, table_id, partition_key_index, column_id,
	                          transform);
}

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
