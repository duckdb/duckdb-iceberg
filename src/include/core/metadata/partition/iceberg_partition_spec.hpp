#pragma once

#include "duckdb/common/types/vector.hpp"

#include "core/expression/iceberg_transform.hpp"
#include "rest_catalog/objects/partition_spec.hpp"
#include "rest_catalog/objects/partition_field.hpp"
#include "rest_catalog/objects/sort_order.hpp"
#include "rest_catalog/objects/sort_field.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

struct IcebergPartitionSpec;

struct IcebergPartitionSpecField {
public:
	static IcebergPartitionSpecField ParseFromJson(const rest_api_objects::PartitionField &field);

private:
	//! Spec field name. Derived using the column source id, the raw transform type, and the column name
	//! by using these three fields names can never collide unless the same transform is used on the same column
	//! Eventually users will be able to choose a partition name themselves.
	string name;
	//! "Applied to the source column(s) to produce a partition value"
	IcebergTransform transform;
	//! NOTE: v3 replaces 'source-id' with 'source-ids'
	//! "A source column id or a list of source column ids from the table’s schema"
	uint64_t source_id;
	//! "Used to identify a partition field and is unique within a partition spec"
	uint64_t partition_field_id;

public:
	void SetPartitionSpecFieldName(string &column_name);
	const string &GetPartitionSpecFieldName() const;
	void SetPartitionSourceId(idx_t _source_id);
	void SetPartitionFieldId(idx_t _field_id);
	void SetPartitionTransform(IcebergTransform &_transform);

	const IcebergTransform &GetIcebergTransform() const;
	const uint64_t &GetSourceId() const;
	const uint64_t &GetPartitionFieldId() const;

	friend IcebergPartitionSpec;
};

struct IcebergPartitionSpec {
public:
	IcebergPartitionSpec(int32_t spec_id) : spec_id(spec_id) {
	}

public:
	static IcebergPartitionSpec ParseFromJson(const rest_api_objects::PartitionSpec &spec);

public:
	bool IsUnpartitioned() const;
	bool IsPartitioned() const;
	const IcebergPartitionSpecField &GetFieldBySourceId(idx_t field_id) const;
	optional_ptr<const IcebergPartitionSpecField> TryGetFieldBySourceId(idx_t field_id) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;
	string FieldsToJSONString() const;
	const vector<IcebergPartitionSpecField> &GetFields() const;
	static yyjson_mut_val *ToJSON(yyjson_mut_doc *doc, const rest_api_objects::PartitionSpec &spec);

private:
	yyjson_mut_val *FieldsToJSON(yyjson_mut_doc *doc) const;

public:
	int32_t spec_id;
	vector<IcebergPartitionSpecField> fields;
};

} // namespace duckdb
