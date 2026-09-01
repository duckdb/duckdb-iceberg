#include "planning/iceberg_optimizer.hpp"

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/optimizer/topn_optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "planning/iceberg_multi_file_list.hpp"
#include "planning/iceberg_multi_file_reader.hpp"

namespace duckdb {

static optional_ptr<LogicalGet> TryGetTopNTableScan(ClientContext &context, LogicalOperator &op) {
	if (!TopN::CanOptimize(op, &context)) {
		return nullptr;
	}
	auto current = op.children[0].get();
	while (current->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		current = current->children[0].get();
	}
	D_ASSERT(current->type == LogicalOperatorType::LOGICAL_ORDER_BY);
	current = current->children[0].get();
	while (current->type == LogicalOperatorType::LOGICAL_PROJECTION ||
	       current->type == LogicalOperatorType::LOGICAL_FILTER) {
		current = current->children[0].get();
	}
	if (current->type != LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	return current->Cast<LogicalGet>();
}

static optional_ptr<IcebergMultiFileList> TryGetIcebergFileList(LogicalGet &get) {
	if (get.function.get_multi_file_reader != IcebergMultiFileReader::CreateInstance || !get.bind_data) {
		return nullptr;
	}
	auto &multi_file_data = get.bind_data->Cast<MultiFileBindData>();
	if (!multi_file_data.file_list) {
		return nullptr;
	}
	return multi_file_data.file_list->Cast<IcebergMultiFileList>();
}

static void IcebergLateMaterializationFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
                                                     vector<unique_ptr<Expression>> &filters) {
	auto &data = bind_data->Cast<MultiFileBindData>();
	MultiFilePushdownInfo info(get.table_index, get.names, get.GetColumnIds(), get.extra_info);
	auto new_list =
	    data.multi_file_reader->ComplexFilterPushdown(context, *data.file_list, data.file_options, info, filters);
	if (new_list) {
		data.file_list = std::move(new_list);
		MultiFileReader::PruneReaders(data, *data.file_list);
	}

	// Filters have been pushed through projections, so aliases now refer to the scan's columns. The core's
	// CreateLHSGet cannot copy a table filter on a virtual column; projection/order-only references are safe.
	for (auto &filter : filters) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    *filter, [&](const BoundColumnRefExpression &ref) {
			    auto &binding = ref.Binding();
			    if (binding.table_index == get.table_index &&
			        get.GetColumnIds()[binding.column_index].IsVirtualColumn()) {
				    get.function.late_materialization = false;
			    }
		    });
	}
}

static void EnableIcebergTopNLateMaterialization(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto get = TryGetTopNTableScan(input.context, *plan);
	if (get) {
		auto file_list = TryGetIcebergFileList(*get);
		if (file_list && file_list->SupportsLateMaterialization()) {
			// When server-side scan planning is enabled, skip late materialization.
			// Server-side planning already optimizes the scan at the catalog level,
			// and late materialization creates separate candidate/payload scans that
			// would complicate the server-side plan.
			Value value;
			bool server_side_enabled = false;
			if (input.context.TryGetCurrentSetting("iceberg_use_server_side_scan_planning", value) && !value.IsNull() &&
			    value.type().id() == LogicalTypeId::BOOLEAN) {
				server_side_enabled = value.GetValue<bool>();
			}
			if (!server_side_enabled) {
				get->function.late_materialization = true;
				get->function.pushdown_complex_filter = IcebergLateMaterializationFilterPushdown;
			}
		}
	}
	for (auto &child : plan->children) {
		EnableIcebergTopNLateMaterialization(input, child);
	}
}

void IcebergOptimizer::PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	// IcebergPlanner::PostBind applies local-planning restrictions before DuckDB's
	// correlation and lateral planning. Only TopN late-materialization setup belongs here.
	EnableIcebergTopNLateMaterialization(input, plan);
}

OptimizerExtension IcebergOptimizer::Create() {
	OptimizerExtension ext;
	ext.pre_optimize_function = IcebergOptimizer::PreOptimize;
	return ext;
}

} // namespace duckdb
