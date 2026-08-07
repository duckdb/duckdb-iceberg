#include "planning/scan_plan/iceberg_scan_plan_provider.hpp"

#include "catalog/rest/api/iceberg_expression.hpp"
#include "catalog/rest/api/iceberg_type.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/storage/table/row_group_reorderer.hpp"
#include "planning/pruning/iceberg_table_filter.hpp"
#include "planning/scan_order/iceberg_scan_order.hpp"

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
		if (table_info.catalog.supported_urls.count(IcebergServerSideScanPlanning::PLAN_ENDPOINT)) {
			rest_api_objects::PlanTableScanRequest request;
			request.snapshot_id = context.snapshot.snapshot->snapshot_id;
			request.case_sensitive = true;
			request.use_snapshot_schema =
			    context.snapshot.snapshot->snapshot_id != context.metadata.current_snapshot_id;
			unique_ptr<rest_api_objects::Expression> server_side_filter;
			for (auto &filter : table_filters) {
				auto primary_index = filter.first.GetPrimaryIndex();
				if (primary_index >= context.schema.columns.size()) {
					continue;
				}
				auto converted = IcebergExpression::TryConvertFilter(*filter.second->expr,
				                                                     context.schema.columns[primary_index]->name);
				server_side_filter =
				    IcebergExpression::AndExpression(std::move(server_side_filter), std::move(converted));
			}
			request.filter = std::move(server_side_filter);

			auto scan_order_options = scan_order.GetOptions();
			if (scan_order_options) {
				if (scan_order_options->row_limit.IsValid()) {
					request.min_rows_requested = NumericCast<int64_t>(scan_order_options->row_limit.GetIndex() +
					                                                  scan_order_options->row_group_offset);
				}
				if (scan_order_options->column_idx.HasPrimaryIndex() &&
				    scan_order_options->column_idx.GetPrimaryIndex() < context.schema.columns.size()) {
					rest_api_objects::FieldName stats_field;
					stats_field.value = context.schema.columns[scan_order_options->column_idx.GetPrimaryIndex()]->name;
					request.stats_fields.emplace();
					request.stats_fields->push_back(std::move(stats_field));
				}
			}

			IcebergServerSideScanPlan plan;
			if (IcebergServerSideScanPlanning::Plan(context.context, table_info, std::move(request), plan)) {
				if (!plan.storage_credentials.empty()) {
					table_info.LoadCredentials(
					    context.context, table_info.GetVendedCredentials(context.context, plan.storage_credentials));
				}
				provider = make_uniq<ServerSideScanPlanProvider>(std::move(plan));
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
