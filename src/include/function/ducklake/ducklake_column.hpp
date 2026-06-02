#pragma once

#include "duckdb/common/constants.hpp"
#include "core/metadata/schema/iceberg_column_definition.hpp"
#include "function/ducklake/ducklake_snapshot.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

struct DuckLakeColumn {
public:
	DuckLakeColumn(const IcebergColumnDefinition &column, idx_t order,
	               optional_ptr<const IcebergColumnDefinition> parent);

public:
	bool IsNested() const;

public:
	bool operator==(const DuckLakeColumn &other);
	bool operator!=(const DuckLakeColumn &other);

public:
	string FinalizeEntry(int64_t table_id, const map<timestamp_t, DuckLakeSnapshot> &snapshots);

public:
	//! This is the 'field_id' of the iceberg schema
	int64_t column_id;
	optional_idx parent_column;

	int64_t column_order;
	string column_name;
	string column_type;
	Value initial_default;
	Value default_value;
	bool nulls_allowed;

	timestamp_t start_snapshot;
	bool has_end = false;
	timestamp_t end_snapshot;
};

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
