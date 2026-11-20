//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/rest/iceberg_rest_backend.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/http_status_code.hpp"
#include "duckdb/common/unordered_set.hpp"

#include "catalog/iceberg_catalog_backend.hpp"
#include "catalog/rest/api/url_utils.hpp"
#include "catalog/rest/iceberg_load_table_cache.hpp"
#include "catalog/rest/storage/iceberg_authorization.hpp"
#include "catalog/rest/iceberg_rest_options.hpp"

namespace duckdb {

namespace rest_api_objects {
class CatalogConfig;
class IcebergErrorResponse;
class StorageCredential;
} // namespace rest_api_objects

class IcebergCatalog;
struct IcebergAttachInput;

//! A catalog backend that speaks the Iceberg REST protocol. It owns everything about the server: the authorization,
//! the base URL and prefix, the endpoints and properties the server advertised in /v1/config, and the LoadTable
//! responses it returned (the MAX_TABLE_STALENESS cache and their ETags).
class IcebergRestBackend : public IcebergCatalogBackend,
                           public IcebergCredentialVending,
                           public IcebergServerScanPlanning {
public:
	static constexpr IcebergBackendTypeId TYPE = "iceberg.rest";
	static constexpr const char *DESCRIPTION = "REST Catalog";

public:
	explicit IcebergRestBackend(const Identifier &catalog_name);
	~IcebergRestBackend() override;

	//! Sets the backend up for an ATTACH: parses the options, authorizes, and negotiates /v1/config
	void Attach(IcebergAttachInput &input);

	//! The REST backend of an Iceberg catalog; throws if the catalog isn't a REST catalog
	static IcebergRestBackend &Get(Catalog &catalog);

public:
	//===--------------------------------------------------------------------===//
	// IcebergCatalogBackend
	//===--------------------------------------------------------------------===//
	string Name() const override {
		return "REST Catalog";
	}
	bool IsInstanceOf(IcebergBackendTypeId id) const override {
		return StringUtil::Equals(id, TYPE) || IcebergCatalogBackend::IsInstanceOf(id);
	}

	bool NamespaceExists(ClientContext &context, const IcebergNamespace &ns) override;
	void CreateNamespace(ClientContext &context, const IcebergNamespace &ns) override;
	vector<IcebergNamespace> ListNamespaces(ClientContext &context, const IcebergNamespace &parent) override;
	IcebergCatalogResult<case_insensitive_map_t<string>> LoadNamespaceProperties(ClientContext &context,
	                                                                             const IcebergNamespace &ns) override;

	string GetEntryKey(const IcebergNamespace &ns, const string &name) const override;
	IcebergCatalogResult<vector<IcebergListedEntry>> ListEntries(ClientContext &context, const IcebergNamespace &ns,
	                                                             IcebergEntryKind kind) override;
	shared_ptr<const IcebergLoadedTable> GetCachedTable(ClientContext &context, const string &table_key) override;
	unique_ptr<IcebergTableLoad> BeginLoadTable(ClientContext &context, IcebergEntryId id,
	                                            const string &table_key) override;
	void OnTableCreated(const string &table_key, shared_ptr<const IcebergLoadedTable> table) override;
	void OnTableChanged(const string &table_key, optional_ptr<const IcebergLoadedTable> table, bool dropped) override;
	IcebergCatalogResult<shared_ptr<const IcebergLoadedView>> LoadView(ClientContext &context,
	                                                                   const IcebergEntryId &id) override;
	const case_insensitive_map_t<string> &GetCatalogProperties() const override {
		return catalog_properties;
	}

	IcebergCreatedTable CreateTable(ClientContext &context, const IcebergEntryId &id, IcebergNewTable &table) override;
	void Commit(ClientContext &context, IcebergCommitPlan &plan) override;
	void Abort(ClientContext &context, IcebergCommitPlan &plan) override;

	optional_ptr<IcebergCredentialVending> GetCredentialVending() override {
		return this;
	}
	optional_ptr<IcebergServerScanPlanning> GetServerScanPlanning() override {
		return this;
	}

	//===--------------------------------------------------------------------===//
	// IcebergCredentialVending
	//===--------------------------------------------------------------------===//
	//! ACCESS_DELEGATION_MODE 'vended_credentials'
	bool VendsCredentials() const override;
	case_insensitive_map_t<Value> GetSecretDefaults(ClientContext &context) override;
	void AdjustConfigSecret(ClientContext &context, CreateSecretInput &info) override;
	unique_ptr<SecretEntry> GetHTTPSecret(ClientContext &context) override;
	IcebergCatalogResult<IcebergVendedCredentials> LoadCredentials(ClientContext &context,
	                                                               const IcebergEntryId &id) override;

	//===--------------------------------------------------------------------===//
	// IcebergServerScanPlanning
	//===--------------------------------------------------------------------===//
	//! Whether the server advertises the plan endpoint
	bool PlansScans() const override;
	bool PlanScan(IcebergScanPlanContext &context, IcebergTable &table, const IcebergTableFilters &filters,
	              const IcebergScanOrder &scan_order, IcebergServerSideScanPlan &result) override;

public:
	//===--------------------------------------------------------------------===//
	// REST protocol
	//===--------------------------------------------------------------------===//
	//! The one place that maps an HTTP status to an outcome category
	static IcebergCatalogStatus MapHTTPStatus(HTTPStatusCode status);
	//! The categorized error of a REST error response, with the message 'operation endpoint returned response code
	//! <status> with message "<message>"'
	static IcebergCatalogError MakeError(const string &operation, HTTPStatusCode status,
	                                     const rest_api_objects::IcebergErrorResponse &error);
	static vector<IcebergStorageCredential>
	ToStorageCredentials(const vector<rest_api_objects::StorageCredential> &credentials);

	//! Parses the ATTACH options of the REST backend, and the generic options into 'input'
	static void ParseOptions(IcebergAttachInput &input, IcebergRestOptions &options,
	                         const unordered_map<string, Value> &properties);
	void ParsePrefix();
	void Configure(ClientContext &context, const rest_api_objects::CatalogConfig &config);
	IRCEndpointBuilder GetBaseUrl() const;
	const Identifier &GetName() const {
		return catalog_name;
	}
	string GetWarehouse() const {
		return warehouse;
	}

public:
	//! The name the catalog is attached as, for messages and logs
	const Identifier catalog_name;
	unique_ptr<IcebergAuthorization> auth_handler;
	//! Base URI of the REST catalog
	string base_uri;
	//! version
	const string version;
	//! optional prefix path components
	vector<string> prefix;
	string namespace_separator = "\x1f";
	//! attach options
	IcebergRestOptions options;
	//! The catalog properties /v1/config returned (defaults, with the overrides applied)
	case_insensitive_map_t<string> catalog_properties;
	unordered_set<string> supported_urls;
	LoadTableResultCache table_request_cache;

private:
	void AddDefaultSupportedEndpoints();
	void AddS3TablesEndpoints();
	void AddGlueEndpoints();

private:
	IcebergEndpointType endpoint_type = IcebergEndpointType::INVALID;
	//! warehouse
	string warehouse;
};

} // namespace duckdb
