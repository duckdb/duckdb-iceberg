#include "catalog/rest/iceberg_access_delegation.hpp"

#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"

#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

optional_ptr<IcebergAccessDelegationProvider> IcebergAccessDelegation::GetActiveProvider(IcebergCatalog &catalog,
                                                                                         ClientContext &context) {
	auto &provider_name = catalog.attach_options.access_delegation_provider;
	if (provider_name.empty()) {
		return nullptr;
	}
	auto registry = IcebergPluginRegistry::TryGet(context);
	if (!registry) {
		return nullptr;
	}
	return registry->GetProvider(provider_name);
}

void IcebergAccessDelegation::ResolveProviderForAttach(IcebergAttachOptions &options, ClientContext &context) {
	if (options.access_delegation_provider.empty()) {
		return;
	}
	auto &provider_name = options.access_delegation_provider;
	auto &registry = IcebergPluginRegistry::GetOrCreate(context);
	auto provider = registry.GetProvider(provider_name);
	if (!provider) {
		// The provider lives in a separate community extension named after the mode; autoload it so
		// `ATTACH ... (ACCESS_DELEGATION_MODE 'lake_formation')` works without an explicit LOAD. Swallow
		// autoload failures here so we can surface a single, clear access-mode error below regardless of
		// whether the extension is missing or simply did not register a provider.
		try {
			ExtensionHelper::AutoLoadExtension(context, provider_name);
			provider = registry.GetProvider(provider_name);
		} catch (std::exception &) {
		}
	}
	if (!provider) {
		throw InvalidConfigurationException(
		    "Unrecognized access mode '%s'. Built-in options are 'vended_credentials' and 'none'; any other "
		    "value must name an extension that registers an Iceberg access-delegation provider, but the '%s' "
		    "extension could not be loaded or did not register one.",
		    provider_name, provider_name);
	}
	provider->ValidateAttachOptions(options);
}

void IcebergAccessDelegation::OnTableLoaded(IcebergTableInformation &table_info, ClientContext &context) {
	auto provider = GetActiveProvider(table_info.catalog, context);
	if (!provider) {
		return;
	}
	IcebergAccessDelegationContext dctx {table_info.catalog, table_info, context};
	provider->OnTableLoaded(dctx);
}

bool IcebergAccessDelegation::PrepareScanCredentials(IcebergTableInformation &table_info, ClientContext &context,
                                                     optional_ptr<const vector<Value>> partition_values) {
	auto provider = GetActiveProvider(table_info.catalog, context);
	if (!provider) {
		return false;
	}
	IcebergAccessDelegationContext dctx {table_info.catalog, table_info, context};
	auto credentials = provider->GetScanCredentials(dctx, partition_values);
	if (credentials.config) {
		auto &secret_manager = SecretManager::Get(context);
		(void)secret_manager.CreateSecret(context, *credentials.config);
	}
	return true;
}

void IcebergAccessDelegation::ApplyMandatoryScanFilters(IcebergTableInformation &table_info, ClientContext &context,
                                                        IcebergScanInfo &scan_info, IcebergMultiFileList &file_list,
                                                        const IcebergTableSchema &schema) {
	auto provider = GetActiveProvider(table_info.catalog, context);
	if (!provider) {
		return;
	}
	IcebergAccessDelegationContext dctx {table_info.catalog, table_info, context};
	provider->ApplyMandatoryScanFilters(dctx, scan_info, file_list, schema);
}

void IcebergAccessDelegation::OnPartitionFile(IcebergTableInformation &table_info, ClientContext &context,
                                              const vector<IcebergPartitionInfo> &partition_info,
                                              const string &file_path) {
	auto provider = GetActiveProvider(table_info.catalog, context);
	if (!provider) {
		return;
	}
	IcebergAccessDelegationContext dctx {table_info.catalog, table_info, context};
	provider->OnPartitionFile(dctx, partition_info, file_path);
}

} // namespace duckdb
