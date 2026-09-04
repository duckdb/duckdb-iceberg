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

namespace duckdb {

static optional_ptr<LogicalGet> GetTopNSource(ClientContext &context, LogicalOperator &op) {
	if (!TopN::CanOptimize(op, &context)) {
		return nullptr;
	}
	auto current = op.children[0].get();
	while (current->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		current = current->children[0].get();
	}
	if (current->type != LogicalOperatorType::LOGICAL_ORDER_BY) {
		return nullptr;
	}
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

static bool IsIcebergScan(const LogicalGet &get) {
	return get.function.name == "iceberg_scan" &&
	       get.function.get_multi_file_reader == IcebergMultiFileReader::CreateInstance && get.bind_data &&
	       get.function.get_row_id_columns;
}

static bool SupportsLateMaterialization(const LogicalGet &get) {
	auto &multi_file_data = get.bind_data->Cast<MultiFileBindData>();
	if (!multi_file_data.file_list) {
		return false;
	}
	auto &file_list = multi_file_data.file_list->Cast<IcebergMultiFileList>();
	return file_list.SupportsLateMaterialization();
}

static void EnableTopNLateMaterialization(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
	auto get = GetTopNSource(context, *plan);
	if (get && IsIcebergScan(*get) && SupportsLateMaterialization(*get)) {
		get->function.late_materialization = true;
	}
	for (auto &child : plan->children) {
		EnableTopNLateMaterialization(context, child);
	}
}

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

void IcebergOptimizer::PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	IcebergOptimizerRoutine iceberg_optimizer_routine(input.context);
	if (plan->children.size() == 0) {
		return;
	}
	iceberg_optimizer_routine.VisitOperator(plan);
	EnableTopNLateMaterialization(input.context, plan);
}

OptimizerExtension IcebergOptimizer::Create() {
	OptimizerExtension ext;
	ext.pre_optimize_function = IcebergOptimizer::PreOptimize;
	return ext;
}

} // namespace duckdb
