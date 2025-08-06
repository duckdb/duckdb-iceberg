#pragma once

#include "metadata/iceberg_column_definition.hpp"
#include "rest_catalog/objects/schema.hpp"
#include "duckdb/common/column_index.hpp"

namespace duckdb {

class IcebergTableSchema {
public:
	static shared_ptr<IcebergTableSchema> ParseSchema(rest_api_objects::Schema &schema);

public:
	static void PopulateSourceIdMap(unordered_map<uint64_t, ColumnIndex> &source_to_column_id,
	                                const vector<unique_ptr<IcebergColumnDefinition>> &columns,
	                                optional_ptr<ColumnIndex> parent);
	static const IcebergColumnDefinition &GetFromColumnIndex(const vector<unique_ptr<IcebergColumnDefinition>> &columns,
	                                                         const ColumnIndex &column_index, idx_t depth);

	static void SchemaToJson(yyjson_mut_doc *doc, yyjson_mut_val *root_object, const rest_api_objects::Schema &schema);

public:
	int32_t schema_id;
	vector<unique_ptr<IcebergColumnDefinition>> columns;
};

} // namespace duckdb
