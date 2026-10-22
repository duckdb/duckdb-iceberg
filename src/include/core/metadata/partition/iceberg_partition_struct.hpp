//===----------------------------------------------------------------------===//
//                         DuckDB
//
// core/metadata/partition/iceberg_partition_struct.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"

namespace duckdb {

//! A field of the 'partition' struct
struct IcebergPartitionStructField {
	string name;
	LogicalType type;
	reference<const IcebergPartitionSpecField> field;
};

//! The 'partition' struct of the metadata tables
struct IcebergPartitionStruct {
	static vector<IcebergPartitionStructField> GetFields(const IcebergTableMetadata &metadata);
	static LogicalType GetType(const vector<IcebergPartitionStructField> &fields);
	static Value GetValue(const vector<IcebergPartitionStructField> &fields, const IcebergDataFile &data_file);
};

} // namespace duckdb
