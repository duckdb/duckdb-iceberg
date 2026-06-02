#include "planning/iceberg_optimizer.hpp"

#include "iceberg_logging.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "core/metadata/schema/iceberg_column_definition.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "planning/iceberg_multi_file_list.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

GuaranteeEqualityDeleteColumnsOptimizer::GuaranteeEqualityDeleteColumnsOptimizer(ClientContext &context)
    : context(context) {
}

void GuaranteeEqualityDeleteColumnsOptimizer::VisitOperator(unique_ptr<LogicalOperator> &op) {
	for (idx_t child_index = 0; child_index < op->children.size(); child_index++) {
		auto &child = op->children[child_index];
		if (child->type != LogicalOperatorType::LOGICAL_GET) {
			VisitOperator(child);
			return;
		}
		auto &get = child->Cast<LogicalGet>();
		if (get.function.name != "iceberg_scan" || !get.bind_data) {
			VisitOperator(child);
			return;
		}
		auto &mfbd = get.bind_data->Cast<MultiFileBindData>();
		if (!mfbd.file_list) {
			return;
		}
		auto &iceberg_list = mfbd.file_list->Cast<IcebergMultiFileList>();
		{
			lock_guard<mutex> guard(iceberg_list.delete_lock);
			iceberg_list.EnumerateDeleteManifestEntries();
		}

		unordered_set<int32_t> required_field_ids;
		for (auto &entry : iceberg_list.delete_manifest_entries) {
			auto &mft = entry.entry;
			if (mft.data_file.content != IcebergManifestEntryContentType::EQUALITY_DELETES) {
				continue;
			}
			for (auto fid : mft.data_file.equality_ids) {
				required_field_ids.insert(fid);
			}
		}

		if (required_field_ids.empty()) {
			return;
		}

		auto &schema_columns = iceberg_list.GetSchema().columns;
		LogicalType col_type;
		vector<unique_ptr<Expression>> args;
		for (auto fid : required_field_ids) {
			idx_t schema_idx = DConstants::INVALID_INDEX;
			for (idx_t i = 0; i < schema_columns.size(); i++) {
				if (schema_columns[i]->id == fid) {
					schema_idx = i;
					col_type = schema_columns[schema_idx]->type;
					break;
				}
			}
			if (schema_idx == DConstants::INVALID_INDEX) {
				// column was deleted and exists most likely in an old schemas
				// TODO: if the type of the equality delete column was evolved, then grabbing just any schema could be a
				// problem
				auto col_info = iceberg_list.table->table_info.table_metadata.FindColumnByFieldId(fid);
				if (!col_info) {
					throw InvalidConfigurationException(
					    "column %d must apply equality deletes, but no schema has a column with that field id", fid);
				}
				DUCKDB_LOG(context, IcebergLogType, "Detected deleted column with equality delete: %s", col_info->name);
				schema_idx = col_info->id;
				col_type = col_info->type;

				// modify the returned types of the get to add a column
				get.returned_types.push_back(col_type);

				// modify the multi file reader bind data to add the extra column
				mfbd.types.push_back(col_type);
				mfbd.names.push_back(col_info->name);

				auto new_col = col_info->GetMultiFileColumnDefinition();
				if (!new_col.default_expression) {
					// set default expression to null.
					new_col.default_expression = make_uniq<ConstantExpression>(Value(col_type));
				}
				new_col.identifier = col_info->id;
				mfbd.columns.push_back(new_col);
				// also push back the info to the reader_bind.schema
				mfbd.reader_bind.schema.push_back(new_col);
			}
			idx_t local_idx = DConstants::INVALID_INDEX;
			const auto &col_ids = get.GetColumnIds();
			for (idx_t i = 0; i < col_ids.size(); i++) {
				if (!col_ids[i].IsVirtualColumn() && col_ids[i].GetPrimaryIndex() == schema_idx) {
					local_idx = i;
					break;
				}
			}
			if (local_idx == DConstants::INVALID_INDEX) {
				get.AddColumnId(schema_idx);
				local_idx = get.GetColumnIds().size() - 1;
			}
			auto bindings = get.GetColumnBindings();
			args.push_back(make_uniq<BoundColumnRefExpression>(col_type, bindings[local_idx]));
		}
		if (args.empty()) {
			return;
		}

		auto &catalog = Catalog::GetSystemCatalog(context);
		auto &fn_entry =
		    catalog.GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "iceberg_verify_equality_deletes");
		FunctionBinder function_binder(context);
		vector<LogicalType> arg_types;
		for (auto &a : args) {
			arg_types.push_back(a->GetReturnType());
		}
		auto fn = fn_entry.functions.GetFunctionByArguments(context, arg_types);
		auto bound_call = function_binder.BindScalarFunction(fn, std::move(args));

		auto filter = make_uniq<LogicalFilter>();
		filter->expressions.push_back(std::move(bound_call));
		filter->children.push_back(std::move(op->children[child_index]));
		op->children[child_index] = std::move(filter);
	}
}

void IcebergOptimizer::PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	GuaranteeEqualityDeleteColumnsOptimizer guarantee_equality_delete_columns_optimizer(input.context);
	if (plan->children.size() == 0) {
		return;
	}
	guarantee_equality_delete_columns_optimizer.VisitOperator(plan);
}

OptimizerExtension IcebergOptimizer::Create() {
	OptimizerExtension ext;
	ext.pre_optimize_function = IcebergOptimizer::PreOptimize;
	return ext;
}

} // namespace duckdb
