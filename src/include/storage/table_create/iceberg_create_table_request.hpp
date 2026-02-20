#pragma once

#include "catalog_utils.hpp"
#include "storage/iceberg_table_update.hpp"
#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_table_schema.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_snapshot.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

using namespace duckdb_yyjson;
namespace duckdb {

struct YyjsonDocDeleter;
struct IcebergTableInformation;
class IcebergTableEntry;

struct IcebergCreateTableRequest {
	explicit IcebergCreateTableRequest(const IcebergTableInformation &table_info);

public:
	static shared_ptr<IcebergTableSchema> CreateIcebergSchema(const IcebergTableEntry *table_entry);
	string CreateTableToJSON(std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p);
	static void PopulateSchema(yyjson_mut_doc *doc, yyjson_mut_val *schema_json, IcebergTableSchema &schema);

private:
	const IcebergTableInformation &table_info;
};

} // namespace duckdb
