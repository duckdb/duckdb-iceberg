#pragma once

#include "duckdb/common/column_index.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/types/value.hpp"

#include "core/metadata/schema/iceberg_column_definition.hpp"
#include "rest_catalog/objects/schema.hpp"
#include "rest_catalog/objects/add_schema_update.hpp"

namespace duckdb {

class IcebergTableSchema {
public:
	static shared_ptr<IcebergTableSchema> ParseSchema(const rest_api_objects::Schema &schema);
	rest_api_objects::Schema ToRESTObject() const;

public:
	static const IcebergColumnDefinition &GetFromColumnIndex(const vector<unique_ptr<IcebergColumnDefinition>> &columns,
	                                                         const ColumnIndex &column_index, idx_t depth);
	const unordered_map<uint64_t, ColumnIndex> &GetSourceIdMap() const;
	optional<ColumnIndex> TryGetColumnIndexByFieldId(idx_t field_id) const;
	optional_ptr<const IcebergColumnDefinition> TryGetColumnByFieldId(idx_t field_id) const;
	const IcebergColumnDefinition &GetColumnByFieldId(idx_t field_id) const;
	optional_ptr<const IcebergColumnDefinition> GetFromPath(const vector<Identifier> &path,
	                                                        optional_ptr<optional_idx> names_offset) const;
	optional_ptr<IcebergColumnDefinition> GetMutableFromPath(const vector<Identifier> &path,
	                                                         optional_ptr<optional_idx> names_offset);

	shared_ptr<IcebergTableSchema> Copy() const;
	shared_ptr<IcebergTableSchema> RemoveColumn(const string &name, optional_idx &column_id) const;
	const LogicalType &GetColumnTypeFromFieldId(idx_t field_id) const;

	bool Equals(const IcebergTableSchema &other) const;
	void GetColumnNamesAndTypes(vector<string> &names, vector<LogicalType> &types) const;
	void GetFieldIdValues(child_list_t<Value> &values) const;
	Value GetFieldIds() const;

private:
	void InvalidateSourceIdMap();
	static void PopulateSourceIdMap(unordered_map<uint64_t, ColumnIndex> &source_to_column_id,
	                                const vector<unique_ptr<IcebergColumnDefinition>> &columns,
	                                optional_ptr<ColumnIndex> parent);

private:
	mutable mutex source_id_map_lock;
	mutable bool source_id_map_populated = false;
	mutable unordered_map<uint64_t, ColumnIndex> source_to_column_id;

public:
	int32_t schema_id;
	// Nessie Needs this for some reason.
	idx_t last_column_id;
	vector<unique_ptr<IcebergColumnDefinition>> columns;
	vector<int32_t> identifier_field_ids;
};

} // namespace duckdb
