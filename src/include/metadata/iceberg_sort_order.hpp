#pragma once

#include "metadata/iceberg_transform.hpp"
#include "duckdb/common/types/vector.hpp"

#include "rest_catalog/objects/sort_order.hpp"
#include "rest_catalog/objects/sort_field.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

struct IcebergSortOrderField {
public:
	static IcebergSortOrderField ParseFromJson(rest_api_objects::SortField &field);

public:
	//! the source id of the field (field_id)
	//! NOTE: v3 replaces 'source-id' with 'source-ids'
	int32_t source_id;
	//! used to produce values to be sorted on from the source column(s)
	//! The same as a transform for partition values
	IcebergTransform transform;
	//! "A sort direction that can either be ASC or DESC"
	string direction;
	//! "A null order that describes the order of null values when sorted, Can either be 'nulls-first' or 'nulls-last'"
	string null_order;
};

struct IcebergSortOrder {
public:
	static IcebergSortOrder ParseFromJson(rest_api_objects::SortOrder &sort_order_spec);

	bool IsSorted() const;

public:
	int32_t sort_order_id;
	vector<IcebergSortOrderField> fields;
};

} // namespace duckdb
