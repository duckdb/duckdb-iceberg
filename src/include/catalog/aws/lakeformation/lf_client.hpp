#pragma once

#include "catalog/aws/lakeformation/lf_types.hpp"
#include "catalog/aws/lakeformation/lf_credentials.hpp"
#include "catalog/aws/lakeformation/lf_credential_cache.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {

class IcebergCatalog;
class IcebergSchemaEntry;
class IcebergTableInformation;

struct LakeFormationTableIdentifiers {
	string catalog_id;
	string database_name;
	string table_name;
	string table_arn;
	string region;
};

class LakeFormationClient {
public:
	explicit LakeFormationClient(ClientContext &context, IcebergCatalog &catalog);

	LakeFormationTableIdentifiers GetTableIdentifiers(IcebergSchemaEntry &schema, const string &table_name) const;
	LakeFormationTablePolicy FetchTablePolicy(IcebergSchemaEntry &schema, const string &table_name,
	                                          const IcebergTableSchema &iceberg_schema,
	                                          const IcebergTableMetadata &table_metadata);
	LakeFormationTemporaryCredentials GetTableCredentials(const LakeFormationTableIdentifiers &identifiers,
	                                                     const LakeFormationTablePolicy &policy,
	                                                     const string &s3_path = "");
	LakeFormationTemporaryCredentials GetPartitionCredentials(const LakeFormationTableIdentifiers &identifiers,
	                                                        const LakeFormationTablePolicy &policy,
	                                                        const vector<Value> &partition_values,
	                                                        const string &s3_path = "");

private:
	ClientContext &context;
	IcebergCatalog &catalog;
	LakeFormationClientConfig client_config;
	LakeFormationCredentialCache credential_cache;
};

} // namespace duckdb
