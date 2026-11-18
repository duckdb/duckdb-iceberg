#include "catalog/rest/iceberg_rest_backend.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/storage/table/row_group_reorderer.hpp"

#include "catalog/rest/api/iceberg_expression.hpp"
#include "catalog/rest/api/iceberg_scan_planning.hpp"
#include "planning/pruning/iceberg_table_filter.hpp"
#include "planning/scan_order/iceberg_scan_order.hpp"
#include "planning/scan_plan/iceberg_scan_plan_context.hpp"

namespace duckdb {

bool IcebergRestBackend::PlansScans() const {
	return supported_urls.count(IcebergServerSideScanPlanning::PLAN_ENDPOINT);
}

bool IcebergRestBackend::PlanScan(IcebergScanPlanContext &context, IcebergTable &table,
                                  const IcebergTableFilters &table_filters, const IcebergScanOrder &scan_order,
                                  IcebergServerSideScanPlan &result) {
	rest_api_objects::PlanTableScanRequest request;
	request.snapshot_id = context.snapshot.snapshot->snapshot_id;
	request.case_sensitive = true;
	request.use_snapshot_schema = context.snapshot.snapshot->snapshot_id != context.metadata.current_snapshot_id;
	unique_ptr<rest_api_objects::Predicate> server_side_filter;
	for (auto &filter : table_filters) {
		auto primary_index = filter.first.GetPrimaryIndex();
		if (primary_index >= context.schema.columns.size()) {
			continue;
		}
		auto converted =
		    IcebergExpression::TryConvertFilter(*filter.second->expr, context.schema.columns[primary_index]->name);
		server_side_filter = IcebergExpression::AndExpression(std::move(server_side_filter), std::move(converted));
	}
	request.filter = std::move(server_side_filter);

	auto scan_order_options = scan_order.GetOptions();
	if (scan_order_options) {
		if (scan_order_options->row_limit.IsValid()) {
			request.min_rows_requested =
			    NumericCast<int64_t>(scan_order_options->row_limit.GetIndex() + scan_order_options->row_group_offset);
		}
		if (scan_order_options->column_idx.HasPrimaryIndex() &&
		    scan_order_options->column_idx.GetPrimaryIndex() < context.schema.columns.size()) {
			rest_api_objects::FieldName stats_field;
			stats_field.value = context.schema.columns[scan_order_options->column_idx.GetPrimaryIndex()]->name;
			request.stats_fields.emplace();
			request.stats_fields->push_back(std::move(stats_field));
		}
	}
	return IcebergServerSideScanPlanning::Plan(context.context, table, std::move(request), result);
}

} // namespace duckdb
