#include "planning/iceberg_optimizer.hpp"

#include "iceberg_logging.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "core/metadata/schema/iceberg_column_definition.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "planning/iceberg_multi_file_list.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/optimizer/topn_optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

IcebergOptimizerRoutine::IcebergOptimizerRoutine(ClientContext &context) : context(context) {
}

void IcebergOptimizerRoutine::VisitOperator(unique_ptr<LogicalOperator> &op) {
	VisitOperator(op, false);
}

void IcebergOptimizerRoutine::VisitOperator(unique_ptr<LogicalOperator> &op, bool below_write) {
	below_write = below_write || op->type == LogicalOperatorType::LOGICAL_INSERT ||
	              op->type == LogicalOperatorType::LOGICAL_DELETE || op->type == LogicalOperatorType::LOGICAL_UPDATE ||
	              op->type == LogicalOperatorType::LOGICAL_MERGE_INTO;
	for (idx_t child_index = 0; child_index < op->children.size(); child_index++) {
		auto &child = op->children[child_index];
		if (child->type != LogicalOperatorType::LOGICAL_GET) {
			VisitOperator(child, below_write);
			continue;
		}
		auto &get = child->Cast<LogicalGet>();
		// Identify our iceberg scan by the multi file reader it installs, not by
		// function name alone. Other extensions might create their own
		// iceberg_scan function or overload ours, so we cannot just depend on
		// the name. We avoid dynamic_cast here because it does not behave
		// reliably across the extension linking boundary; instead the function
		// pointer uniquely identifies our scan, which guarantees the bind data
		// and file list are the iceberg types we expect.
		if (get.function.name != "iceberg_scan" ||
		    get.function.get_multi_file_reader != IcebergMultiFileReader::CreateInstance || !get.bind_data) {
			VisitOperator(child, below_write);
			continue;
		}
		auto &mfbd = get.bind_data->Cast<MultiFileBindData>();
		if (!mfbd.file_list) {
			continue;
		}
		auto &iceberg_list = mfbd.file_list->Cast<IcebergMultiFileList>();
		bool requires_local_planning = below_write;
		for (auto &column_id : get.GetColumnIds()) {
			if (column_id.IsVirtualColumn() &&
			    column_id.GetPrimaryIndex() == IcebergMultiFileReader::COLUMN_IDENTIFIER_LAST_SEQUENCE_NUMBER) {
				requires_local_planning = true;
				break;
			}
		}
		if (requires_local_planning) {
			iceberg_list.DisableServerSidePlanning();
		}
	}
}

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
	if (get.function.get_multi_file_reader != IcebergMultiFileReader::CreateInstance) {
		return nullptr;
	}
	D_ASSERT(get.bind_data);
	auto &multi_file_data = get.bind_data->Cast<MultiFileBindData>();
	D_ASSERT(multi_file_data.file_list);
	return multi_file_data.file_list->Cast<IcebergMultiFileList>();
}

static void IcebergLateMaterializationFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
                                                     vector<unique_ptr<Expression>> &filters) {
	auto &data = bind_data->Cast<MultiFileBindData>();
	MultiFilePushdownInfo info(get);
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
			get->function.late_materialization = true;
			get->function.pushdown_complex_filter = IcebergLateMaterializationFilterPushdown;
		}
	}
	for (auto &child : plan->children) {
		EnableIcebergTopNLateMaterialization(input, child);
	}
}

void IcebergOptimizer::PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	IcebergOptimizerRoutine iceberg_optimizer_routine(input.context);
	if (plan->children.size() == 0) {
		return;
	}
	iceberg_optimizer_routine.VisitOperator(plan);
	// Keep local-planning restrictions ahead of TopN cardinality estimation, which may initialize scan planning.
	EnableIcebergTopNLateMaterialization(input, plan);
}

OptimizerExtension IcebergOptimizer::Create() {
	OptimizerExtension ext;
	ext.pre_optimize_function = IcebergOptimizer::PreOptimize;
	return ext;
}

} // namespace duckdb
