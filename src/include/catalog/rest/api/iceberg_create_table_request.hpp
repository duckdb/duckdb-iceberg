#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

#include "catalog/rest/api/catalog_utils.hpp"
#include "catalog/rest/api/iceberg_table_update.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"

using namespace duckdb_yyjson;
namespace duckdb {

struct YyjsonDocDeleter;
struct IcebergTableInformation;
class IcebergTableEntry;

struct IcebergCreateTableRequest {
	explicit IcebergCreateTableRequest(const IcebergTableInformation &table_info);

public:
	static shared_ptr<IcebergTableSchema>
	CreateIcebergSchema(ClientContext &context, const IcebergTableMetadata &table_metadata, const ColumnList &columns,
	                    optional_ptr<const vector<unique_ptr<Constraint>>> constraints, int32_t &last_column_id);
	string CreateTableToJSON(std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p);
	static void PopulateSchema(yyjson_mut_doc *doc, yyjson_mut_val *schema_json, const IcebergTableSchema &schema);

private:
	const IcebergTableInformation &table_info;
};

} // namespace duckdb
