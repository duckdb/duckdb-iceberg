#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types/vector.hpp"

#include "core/expression/iceberg_transform.hpp"
#include "rest_catalog/objects/partition_spec.hpp"
#include "rest_catalog/objects/partition_field.hpp"
#include "rest_catalog/objects/sort_order.hpp"
#include "rest_catalog/objects/sort_field.hpp"

namespace duckdb {

struct IcebergPartitionSpecField {
public:
	//! 'assigned_field_id' replaces the field-id that V1 partition fields don't have to store (see
	//! IcebergPartitionSpec::ParseFromJson)
	static IcebergPartitionSpecField ParseFromJson(const rest_api_objects::PartitionField &field,
	                                               optional_idx assigned_field_id);

public:
	//! "Applied to the source column(s) to produce a partition value"
	IcebergTransform transform;
	//! NOTE: v3 replaces ‘source-id’ with ‘source-ids’
	//! "A source column id or a list of source column ids from the table’s schema"
	uint64_t source_id;
	//! "Used to identify a partition field and is unique within a partition spec"
	uint64_t partition_field_id;

	//! Field equivalence for unnamed partition expressions: source ID and full transform, including parameters.
	//! Partition field IDs and names are deliberately ignored so historical fields can be reused.
	bool Equals(const IcebergPartitionSpecField &other) const;

	//! Sets the field name, derived from transform + column_name + source_id with Avro-compliance sanitization.
	//! Must be called after transform and source_id are set.
	void SetPartitionSpecFieldName(const string &column_name);
	const string &GetPartitionSpecFieldName() const;

private:
	//! Spec field name. Derived using the column source id, the raw transform type, and the column name.
	//! By using these three fields, names can never collide unless the same transform is used on the same column.
	//! Eventually users will be able to choose a partition name themselves.
	string name;
};

struct IcebergPartitionSpec {
public:
	IcebergPartitionSpec(int32_t spec_id) : spec_id(spec_id) {
	}

public:
	static IcebergPartitionSpec ParseFromJson(const rest_api_objects::PartitionSpec &spec, int32_t iceberg_version);

public:
	bool IsUnpartitioned() const;
	bool IsPartitioned() const;
	const IcebergPartitionSpecField &GetFieldBySourceId(idx_t source_id) const;
	optional_ptr<const IcebergPartitionSpecField> TryGetFieldBySourceId(idx_t source_id) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;
	string FieldsToJSONString() const;
	const vector<IcebergPartitionSpecField> &GetFields() const;
	//! Spec compatibility: ordered source IDs, full transforms, and field names must match.
	//! Spec IDs and partition field IDs are ignored, like Java's PartitionSpec.compatibleWith.
	bool Equals(const IcebergPartitionSpec &other) const;

private:
	JSONMutableValue FieldsToJSON(JSONWriter &writer) const;

public:
	int32_t spec_id;
	vector<IcebergPartitionSpecField> fields;
};

} // namespace duckdb
