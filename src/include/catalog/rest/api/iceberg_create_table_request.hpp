#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

#include "catalog/rest/api/catalog_utils.hpp"
#include "catalog/rest/api/iceberg_table_update.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "common/iceberg_default.hpp"

using namespace duckdb_yyjson;
namespace duckdb {

struct YyjsonDocDeleter;

struct IcebergCreateTableRequest {
	IcebergCreateTableRequest(string name, shared_ptr<IcebergTableSchema> schema, IcebergPartitionSpec partition_spec,
	                          idx_t iceberg_version, case_insensitive_map_t<string> table_properties, string location);

public:
	static unique_ptr<IcebergColumnDefinition>
	CreateIcebergColumn(const ColumnDefinition &coldef, IcebergDefaultBinder &default_binder, bool is_required,
	                    const std::function<idx_t(void)> &next_field_id, idx_t iceberg_version);

	static shared_ptr<IcebergTableSchema>
	CreateIcebergSchema(ClientContext &context, const IcebergTableMetadata &table_metadata, const ColumnList &columns,
	                    optional_ptr<const vector<unique_ptr<Constraint>>> constraints, int32_t &last_column_id);
	string CreateTableToJSON(bool stage_create) const;
	static void PopulateSchema(yyjson_mut_doc *doc, yyjson_mut_val *schema_json, const IcebergTableSchema &schema);

private:
	string name;
	shared_ptr<IcebergTableSchema> schema;
	IcebergPartitionSpec partition_spec;
	idx_t iceberg_version;
	case_insensitive_map_t<string> table_properties;
	string location;
};

} // namespace duckdb
