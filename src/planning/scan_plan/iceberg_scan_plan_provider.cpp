#include "planning/scan_plan/iceberg_scan_plan_provider.hpp"

#include "catalog/rest/api/iceberg_type.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/storage/table/row_group_reorderer.hpp"
#include "planning/pruning/iceberg_table_filter.hpp"
#include "planning/scan_order/iceberg_scan_order.hpp"
#include "duckdb/logging/logger.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {

namespace {

enum class ScanPlanningMode : uint8_t { UNSPECIFIED, SERVER_SIDE_ONLY, CLIENT_SIDE_ONLY };

static ScanPlanningMode GetScanPlanningMode(optional_ptr<IcebergTableSchemaVersion> table) {
	if (!table) {
		return ScanPlanningMode::CLIENT_SIDE_ONLY;
	}

	auto &config = table->table_info.config;
	auto it = config.find("scan-planning-mode");
	if (it == config.end()) {
		return ScanPlanningMode::UNSPECIFIED;
	}
	auto &mode = it->second;
	if (StringUtil::CIEquals(mode, "client")) {
		return ScanPlanningMode::CLIENT_SIDE_ONLY;
	}
	if (StringUtil::CIEquals(mode, "server")) {
		return ScanPlanningMode::SERVER_SIDE_ONLY;
	}
	throw InvalidConfigurationException("Table's config 'scan-planning-mode' has unrecognized option: %s", mode);
}

} // namespace

unique_ptr<IcebergScanPlanProvider> IcebergScanPlanProvider::Create(IcebergScanPlanState &shared_state,
                                                                    IcebergScanPlanContext context,
                                                                    optional_ptr<IcebergTableSchemaVersion> table_entry,
                                                                    const IcebergTableFilters &table_filters,
                                                                    const IcebergScanOrder &scan_order,
                                                                    bool server_side_planning_enabled) {
	if (!context.snapshot.snapshot) {
		return make_uniq<ClientSideScanPlanProvider>(shared_state, context);
	}

	auto scan_planning_mode = GetScanPlanningMode(table_entry);
	if (scan_planning_mode == ScanPlanningMode::UNSPECIFIED) {
		Value value;
		if (context.context.TryGetCurrentSetting("iceberg_use_server_side_scan_planning", value) && !value.IsNull() &&
		    value.type().id() == LogicalTypeId::BOOLEAN && !value.GetValue<bool>()) {
			scan_planning_mode = ScanPlanningMode::CLIENT_SIDE_ONLY;
		}
	}
	if (!table_entry || context.transaction_data || table_entry->table_info.IsRenamed() ||
	    scan_planning_mode == ScanPlanningMode::CLIENT_SIDE_ONLY) {
		server_side_planning_enabled = false;
	}

	unique_ptr<IcebergScanPlanProvider> provider;
	if (server_side_planning_enabled) {
		auto &table_info = table_entry->table_info;
		auto planning = table_info.catalog.GetServerScanPlanning();
		if (planning && planning->PlansScans()) {
			IcebergServerSideScanPlan plan;
			try {
				if (planning->PlanScan(context, table_info, table_filters, scan_order, plan)) {
					if (shared_state.Configuration().row_ids_required && context.metadata.iceberg_version >= 3) {
						// Without inspecting Parquet we cannot know whether all row IDs are
						// materialized. Fall back rather than silently replacing inherited IDs.
						for (auto &manifest : plan.data_manifests) {
							for (auto &entry : manifest.GetManifestEntries()) {
								if (!entry.data_file.HasFirstRowId()) {
									throw InvalidInputException("Server scan plan is missing first-row-id for file "
									                            "'%s', but this scan requires row IDs",
									                            entry.data_file.file_path);
								}
							}
						}
					}
					if (!plan.storage_credentials.empty()) {
						table_info.LoadCredentials(context.context, table_info.GetVendedCredentials(
						                                                context.context, plan.storage_credentials));
					}
					provider =
					    make_uniq<ServerSideScanPlanProvider>(std::move(plan), context.metadata, context.context);
				}
			} catch (std::exception &ex) {
				ErrorData error(ex);
				DUCKDB_LOG_INFO(context.context, "Scan planning failed, Plan resulted in error: %s", error.Message());
			} catch (...) {
				DUCKDB_LOG_INFO(context.context, "Scan planning failed, Plan resulted in unknown error");
			}
		}
	}
	if (!provider && scan_planning_mode == ScanPlanningMode::SERVER_SIDE_ONLY) {
		D_ASSERT(table_entry);
		throw BinderException(
		    "Unable to plan scan for table %s, but table's config disabled non-server-side scan planning",
		    table_entry->table_info.name);
	}
	if (!provider) {
		provider = make_uniq<ClientSideScanPlanProvider>(shared_state, context);
	}
	return provider;
}

} // namespace duckdb
