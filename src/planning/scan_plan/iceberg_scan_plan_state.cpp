#include "planning/scan_plan/iceberg_scan_plan_state.hpp"
#include "planning/scan_plan/iceberg_manifest_store.hpp"
#include "planning/scan_plan/iceberg_scan_plan_provider.hpp"

namespace duckdb {

IcebergScanPlanState::IcebergScanPlanState(ClientContext &context_p, shared_ptr<IcebergScanInfo> scan_info_p,
                                           string path_p, const IcebergOptions &options_p)
    : context(context_p),
      fs(FileSystem::GetFileSystem(context)), configuration {std::move(scan_info_p), std::move(path_p), nullptr,
                                                             options_p, true} {
}

const IcebergScanConfiguration &IcebergScanPlanState::Configuration() const {
	return configuration;
}

void IcebergScanPlanState::RequireConfigurable() const {
	if (configuration_frozen) {
		throw InternalException("Cannot change Iceberg scan configuration after planning or view creation has started");
	}
}

void IcebergScanPlanState::SetScanInfo(shared_ptr<IcebergScanInfo> scan_info) {
	RequireConfigurable();
	configuration.scan_info = std::move(scan_info);
}

void IcebergScanPlanState::SetOptions(const IcebergOptions &options) {
	RequireConfigurable();
	configuration.options = options;
}

void IcebergScanPlanState::SetTable(IcebergTableSchemaVersion &table) {
	RequireConfigurable();
	configuration.table = table;
}

void IcebergScanPlanState::DisableServerSidePlanning() {
	RequireConfigurable();
	configuration.server_side_planning_enabled = false;
}

void IcebergScanPlanState::RequireRowIds() {
	RequireConfigurable();
	configuration.row_ids_required = true;
}

void IcebergScanPlanState::FreezeConfiguration() {
	if (!configuration.scan_info) {
		throw InternalException("Cannot start Iceberg scan planning without scan metadata");
	}
	configuration_frozen = true;
}

IcebergScanPlanState::~IcebergScanPlanState() = default;

IcebergScanPlanProvider &IcebergScanPlanState::GetScanPlanProvider(const IcebergTableFilters &filters,
                                                                   const IcebergScanOrder &scan_order) {
	if (!scan_plan_provider) {
		FreezeConfiguration();
		auto &scan_info = *configuration.scan_info;
		IcebergScanPlanContext plan_context {context,
		                                     fs,
		                                     configuration.path,
		                                     configuration.options,
		                                     scan_info.snapshot_info,
		                                     scan_info.metadata,
		                                     scan_info.schema,
		                                     scan_info.transaction_data.get()};
		scan_plan_provider =
		    IcebergScanPlanProvider::Create(*this, std::move(plan_context), configuration.table, filters, scan_order,
		                                    configuration.server_side_planning_enabled);
	}
	return *scan_plan_provider;
}

IcebergManifestStore &IcebergScanPlanState::GetManifestStore(IcebergScanPlanContext context) {
	FreezeConfiguration();
	if (!manifest_store) {
		manifest_store = make_uniq<IcebergManifestStore>(lock, std::move(context));
	}
	return *manifest_store;
}

} // namespace duckdb
