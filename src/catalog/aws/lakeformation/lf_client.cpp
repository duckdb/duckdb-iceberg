#include "catalog/aws/lakeformation/lf_client.hpp"

#include "catalog/aws/lakeformation/lf_table_metadata.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "duckdb/common/string_util.hpp"

#ifndef EMSCRIPTEN

#include <aws/glue/GlueClient.h>
#include <aws/glue/model/GetUnfilteredPartitionsMetadataRequest.h>
#include <aws/glue/model/GetUnfilteredTableMetadataRequest.h>
#include <aws/lakeformation/LakeFormationClient.h>
#include <aws/lakeformation/model/GetTemporaryGluePartitionCredentialsRequest.h>
#include <aws/lakeformation/model/GetTemporaryGlueTableCredentialsRequest.h>

#include <aws/lakeformation/model/PartitionValueList.h>
#include <aws/glue/model/UnfilteredPartition.h>

namespace duckdb {

namespace {

static string GetDatabaseName(IcebergSchemaEntry &schema) {
	auto schema_component = IRCPathComponent::NamespaceComponent(schema.namespace_items);
	return schema_component.encoded;
}

static string ExtractAccountId(const string &warehouse) {
	auto colon_pos = warehouse.find(':');
	if (colon_pos == string::npos) {
		return "";
	}
	return warehouse.substr(0, colon_pos);
}

static ::Aws::LakeFormation::Model::QuerySessionContext
BuildQuerySessionContext(const LakeFormationTablePolicy &policy) {
	::Aws::LakeFormation::Model::QuerySessionContext context;
	if (!policy.query_authorization_id.empty()) {
		context.SetQueryAuthorizationId(policy.query_authorization_id);
	}
	return context;
}

static vector<::Aws::LakeFormation::Model::Permission> DefaultPermissions() {
	return {::Aws::LakeFormation::Model::Permission::SELECT};
}

static vector<::Aws::LakeFormation::Model::PermissionType> SupportedPermissionTypes() {
	return {::Aws::LakeFormation::Model::PermissionType::CELL_FILTER_PERMISSION};
}

static LakeFormationTemporaryCredentials ParseTemporaryCredentials(
    const ::Aws::LakeFormation::Model::GetTemporaryGlueTableCredentialsResult &result) {
	LakeFormationTemporaryCredentials credentials;
	credentials.access_key_id = result.GetAccessKeyId();
	credentials.secret_access_key = result.GetSecretAccessKey();
	credentials.session_token = result.GetSessionToken();
	credentials.expiration = result.GetExpiration().ToGmtString(::Aws::Utils::DateFormat::ISO_8601);
	return credentials;
}

} // namespace

LakeFormationClient::LakeFormationClient(ClientContext &context, IcebergCatalog &catalog)
    : context(context), catalog(catalog), client_config(BuildLakeFormationClientConfig(context, catalog)) {
}

LakeFormationTableIdentifiers LakeFormationClient::GetTableIdentifiers(IcebergSchemaEntry &schema,
                                                                         const string &table_name) const {
	LakeFormationTableIdentifiers identifiers;
	identifiers.catalog_id = catalog.GetWarehouse();
	identifiers.database_name = GetDatabaseName(schema);
	identifiers.table_name = table_name;
	identifiers.region = client_config.region;

	auto account_id = ExtractAccountId(identifiers.catalog_id);
	if (account_id.empty()) {
		throw InvalidConfigurationException("Could not extract AWS account id from warehouse '%s'",
		                                    identifiers.catalog_id);
	}
	identifiers.table_arn = StringUtil::Format("arn:aws:glue:%s:%s:table/%s/%s", identifiers.region, account_id,
	                                           identifiers.database_name, identifiers.table_name);
	return identifiers;
}

LakeFormationTablePolicy LakeFormationClient::FetchTablePolicy(IcebergSchemaEntry &schema, const string &table_name,
                                                               const IcebergTableSchema &iceberg_schema,
                                                               const IcebergTableMetadata &table_metadata) {
	auto identifiers = GetTableIdentifiers(schema, table_name);

	// LF-filtered reads use two AWS services: Glue returns the caller's effective row/column
	// policy (GetUnfiltered*), while Lake Formation later vends scoped S3 credentials
	// (GetTemporaryGlue*). Both calls require CELL_FILTER_PERMISSION so Glue knows we
	// participate in the data-filter application integration flow.
	::Aws::Glue::GlueClient glue_client(client_config.credentials_provider, client_config.aws_config);
	::Aws::Glue::Model::GetUnfilteredTableMetadataRequest request;
	request.SetCatalogId(identifiers.catalog_id);
	request.SetDatabaseName(identifiers.database_name);
	request.SetName(identifiers.table_name);
	request.SetSupportedPermissionTypes({::Aws::Glue::Model::PermissionType::CELL_FILTER_PERMISSION});

	auto outcome = glue_client.GetUnfilteredTableMetadata(request);
	if (!outcome.IsSuccess()) {
		throw InvalidConfigurationException("GetUnfilteredTableMetadata failed: %s",
		                                  outcome.GetError().GetMessage());
	}

	auto policy = ParseUnfilteredTableMetadata(outcome.GetResult());
	ValidateLakeFormationPolicyV1(policy, iceberg_schema);

	if (!table_metadata.partition_specs.empty()) {
		// Partitioned tables need a second Glue call. LF can return per-partition
		// credentials, so we record partition identity here even though row-filter SQL
		// currently comes from the table-level policy.
		::Aws::Glue::Model::GetUnfilteredPartitionsMetadataRequest partitions_request;
		partitions_request.SetCatalogId(identifiers.catalog_id);
		partitions_request.SetDatabaseName(identifiers.database_name);
		partitions_request.SetTableName(identifiers.table_name);
		partitions_request.SetSupportedPermissionTypes({::Aws::Glue::Model::PermissionType::CELL_FILTER_PERMISSION});

		auto partitions_outcome = glue_client.GetUnfilteredPartitionsMetadata(partitions_request);
		if (!partitions_outcome.IsSuccess()) {
			throw InvalidConfigurationException("GetUnfilteredPartitionsMetadata failed: %s",
			                                  partitions_outcome.GetError().GetMessage());
		}
		policy.is_partitioned = true;
		for (auto &partition : partitions_outcome.GetResult().GetUnfilteredPartitions()) {
			policy.partition_policies.push_back(ParseUnfilteredPartitionMetadata(partition));
		}
	}

	return policy;
}

LakeFormationTemporaryCredentials
LakeFormationClient::GetTableCredentials(const LakeFormationTableIdentifiers &identifiers,
                                         const LakeFormationTablePolicy &policy, const string &s3_path) {
	string cache_key = StringUtil::Format("table:%s:%s:%s", identifiers.catalog_id, identifiers.database_name,
	                                      identifiers.table_name);
	// LF temporary credentials are short-lived; cache within a scan to avoid
	// repeated GetTemporaryGlue* calls for the same table or partition.
	if (auto cached = credential_cache.Get(cache_key)) {
		return *cached;
	}

	::Aws::LakeFormation::LakeFormationClient lf_client(client_config.credentials_provider, client_config.aws_config);
	::Aws::LakeFormation::Model::GetTemporaryGlueTableCredentialsRequest request;
	request.SetTableArn(identifiers.table_arn);
	request.SetPermissions(DefaultPermissions());
	request.SetSupportedPermissionTypes(SupportedPermissionTypes());
	// QueryAuthorizationId links this credential request back to the earlier
	// GetUnfilteredTableMetadata response for the same filtered scan session.
	request.SetQuerySessionContext(BuildQuerySessionContext(policy));
	if (!s3_path.empty()) {
		request.SetS3Path(s3_path);
	}

	auto outcome = lf_client.GetTemporaryGlueTableCredentials(request);
	if (!outcome.IsSuccess()) {
		throw InvalidConfigurationException("GetTemporaryGlueTableCredentials failed: %s",
		                                  outcome.GetError().GetMessage());
	}

	auto credentials = ParseTemporaryCredentials(outcome.GetResult());
	credential_cache.Put(cache_key, credentials);
	return credentials;
}

LakeFormationTemporaryCredentials
LakeFormationClient::GetPartitionCredentials(const LakeFormationTableIdentifiers &identifiers,
                                               const LakeFormationTablePolicy &policy,
                                               const vector<Value> &partition_values, const string &s3_path) {
	string partition_key;
	for (auto &value : partition_values) {
		partition_key += value.ToString() + "|";
	}
	string cache_key = StringUtil::Format("partition:%s:%s:%s:%s", identifiers.catalog_id, identifiers.database_name,
	                                      identifiers.table_name, partition_key);
	if (auto cached = credential_cache.Get(cache_key)) {
		return *cached;
	}

	::Aws::LakeFormation::LakeFormationClient lf_client(client_config.credentials_provider, client_config.aws_config);
	::Aws::LakeFormation::Model::GetTemporaryGluePartitionCredentialsRequest request;
	request.SetTableArn(identifiers.table_arn);
	request.SetPermissions(DefaultPermissions());
	request.SetSupportedPermissionTypes(SupportedPermissionTypes());
	// Unlike table credentials, partition credential requests identify the slice
	// explicitly; QuerySessionContext is not used on this API path.

	::Aws::LakeFormation::Model::PartitionValueList partition_values_list;
	vector<::Aws::String> values;
	for (auto &value : partition_values) {
		values.push_back(value.ToString());
	}
	partition_values_list.SetValues(values);
	request.SetPartition(partition_values_list);

	auto outcome = lf_client.GetTemporaryGluePartitionCredentials(request);
	if (!outcome.IsSuccess()) {
		throw InvalidConfigurationException("GetTemporaryGluePartitionCredentials failed: %s",
		                                  outcome.GetError().GetMessage());
	}

	LakeFormationTemporaryCredentials credentials;
	credentials.access_key_id = outcome.GetResult().GetAccessKeyId();
	credentials.secret_access_key = outcome.GetResult().GetSecretAccessKey();
	credentials.session_token = outcome.GetResult().GetSessionToken();
	credentials.expiration =
	    outcome.GetResult().GetExpiration().ToGmtString(::Aws::Utils::DateFormat::ISO_8601);
	credential_cache.Put(cache_key, credentials);
	return credentials;
}

} // namespace duckdb

#endif
