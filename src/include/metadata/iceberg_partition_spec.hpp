#pragma once

#include "metadata/iceberg_transform.hpp"
#include "duckdb/common/types/vector.hpp"

#include "rest_catalog/objects/partition_spec.hpp"
#include "rest_catalog/objects/partition_field.hpp"
#include "rest_catalog/objects/sort_order.hpp"
#include "rest_catalog/objects/sort_field.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

struct IcebergPartitionSpecField {
public:
	static IcebergPartitionSpecField ParseFromJson(const rest_api_objects::PartitionField &field);

public:
	string name;
	//! "Applied to the source column(s) to produce a partition value"
	IcebergTransform transform;
	//! NOTE: v3 replaces 'source-id' with 'source-ids'
	//! "A source column id or a list of source column ids from the table’s schema"
	uint64_t source_id;
	//! "Used to identify a partition field and is unique within a partition spec"
	uint64_t partition_field_id;
};

struct IcebergPartitionSpec {
public:
	static IcebergPartitionSpec ParseFromJson(const rest_api_objects::PartitionSpec &spec);

public:
	bool IsUnpartitioned() const;
	bool IsPartitioned() const;
	const IcebergPartitionSpecField &GetFieldBySourceId(idx_t field_id) const;
	string FieldsToJSON() const;
	const vector<IcebergPartitionSpecField> &GetFields() const;

public:
	int32_t spec_id;
	vector<IcebergPartitionSpecField> fields;
};

} // namespace duckdb
