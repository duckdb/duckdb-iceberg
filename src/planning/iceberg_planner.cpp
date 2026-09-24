#include "planning/iceberg_planner.hpp"

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
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {

void IcebergPlannerRoutine::VisitOperator(LogicalOperator &op, bool below_write) {
	below_write = below_write || op.type == LogicalOperatorType::LOGICAL_INSERT ||
	              op.type == LogicalOperatorType::LOGICAL_DELETE || op.type == LogicalOperatorType::LOGICAL_UPDATE ||
	              op.type == LogicalOperatorType::LOGICAL_MERGE_INTO;
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		VisitScan(op, below_write);
	}
	// Post-bind runs before subqueries are lowered into operator children.
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expression) {
		ExpressionIterator::VisitExpression<BoundSubqueryExpression>(
		    **expression, [&](const BoundSubqueryExpression &subquery) {
			    if (subquery.Subquery().plan) {
				    VisitOperator(*subquery.Subquery().plan, below_write);
			    }
		    });
	});
	for (auto &child : op.children) {
		VisitOperator(*child, below_write);
	}
}

void IcebergPlannerRoutine::VisitScan(LogicalOperator &op, bool below_write) {
	auto &get = op.Cast<LogicalGet>();
	// Identify our iceberg scan by the multi file reader it installs, not by
	// function name alone. Other extensions might create their own
	// iceberg_scan function or overload ours, so we cannot just depend on
	// the name. We avoid dynamic_cast here because it does not behave
	// reliably across the extension linking boundary; instead the function
	// pointer uniquely identifies our scan, which guarantees the bind data
	// and file list are the iceberg types we expect.
	if (get.function.name != "iceberg_scan" ||
	    get.function.get_multi_file_reader != IcebergMultiFileReader::CreateInstance || !get.bind_data) {
		return;
	}
	auto &mfbd = get.bind_data->Cast<MultiFileBindData>();
	if (!mfbd.file_list) {
		return;
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
		iceberg_list.GetScanPlanner().DisableServerSidePlanning();
	}
}

void IcebergPlanner::PostBind(PlannerExtensionInput &input, BoundStatement &statement) {
	if (statement.plan) {
		IcebergPlannerRoutine routine;
		routine.VisitOperator(*statement.plan);
	}
}

PlannerExtension IcebergPlanner::Create() {
	PlannerExtension ext;
	// Apply scan safety restrictions before optimization starts without invalidating
	// the correlation-domain metadata that DuckDB generates after binding.
	ext.post_bind_function = IcebergPlanner::PostBind;
	return ext;
}

} // namespace duckdb
