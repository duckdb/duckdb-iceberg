#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"

#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "rest_catalog/objects/plan_table_scan_request.hpp"
#include "rest_catalog/objects/storage_credential.hpp"

namespace duckdb {

class ClientContext;
class IcebergCatalog;
struct IcebergTableInformation;

struct IcebergServerSideScanPlan {
	vector<IcebergManifestListEntry> data_manifests;
	vector<IcebergManifestListEntry> delete_manifests;
	//! data-file path -> delete-file paths explicitly referenced by its FileScanTask.
	case_insensitive_map_t<unordered_set<string>> delete_files_by_data_file;
	vector<rest_api_objects::StorageCredential> storage_credentials;
	optional<string> plan_id;
};

class IcebergServerSideScanPlanning {
public:
	static constexpr const char *PLAN_ENDPOINT = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan";
	static constexpr const char *FETCH_ENDPOINT =
	    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}";
	static constexpr const char *CANCEL_ENDPOINT =
	    "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}";
	static constexpr const char *TASKS_ENDPOINT = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks";
	static constexpr const char *CREDENTIALS_ENDPOINT =
	    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials";

	//! Returns false only when the server explicitly declines planning with HTTP 406.
	static bool Plan(ClientContext &context, IcebergTableInformation &table_info,
	                 rest_api_objects::PlanTableScanRequest request, IcebergServerSideScanPlan &result);
};

} // namespace duckdb
