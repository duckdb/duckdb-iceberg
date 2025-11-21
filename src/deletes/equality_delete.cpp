#include "deletes/equality_delete.hpp"
#include "iceberg_multi_file_list.hpp"

#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

namespace duckdb {

static void InitializeFromOtherChunk(DataChunk &target, DataChunk &other, const vector<column_t> &column_ids) {
	vector<LogicalType> types;
	for (auto &id : column_ids) {
		types.push_back(other.data[id].GetType());
	}
	target.InitializeEmpty(types);
}

void IcebergMultiFileList::ScanEqualityDeleteFile(const IcebergManifestEntry &entry, DataChunk &result_p,
                                                  vector<MultiFileColumnDefinition> &local_columns,
                                                  const vector<MultiFileColumnDefinition> &global_columns,
                                                  const vector<ColumnIndex> &column_indexes) const {
	D_ASSERT(!entry.equality_ids.empty());
	D_ASSERT(result_p.ColumnCount() == local_columns.size());

	auto count = result_p.size();
	if (count == 0) {
		return;
	}

	//! Map from column_id to 'local_columns' index, to figure out which columns from the 'result_p' are relevant here
	unordered_map<int32_t, column_t> id_to_column;
	for (column_t i = 0; i < local_columns.size(); i++) {
		auto &col = local_columns[i];
		D_ASSERT(!col.identifier.IsNull());
		id_to_column[col.identifier.GetValue<int32_t>()] = i;
	}

	vector<column_t> column_ids;
	DataChunk result;
	for (auto id : entry.equality_ids) {
		D_ASSERT(id_to_column.count(id));
		column_ids.push_back(id_to_column[id]);
	}

	//! Get or create the equality delete data for this sequence number
	auto it = equality_delete_data.find(entry.sequence_number);
	if (it == equality_delete_data.end()) {
		it = equality_delete_data
		         .emplace(entry.sequence_number, make_uniq<IcebergEqualityDeleteData>(entry.sequence_number))
		         .first;
	}
	auto &deletes = *it->second;

	//! Map from column_id to 'global_columns' index, so we can create a reference to the correct global index
	unordered_map<int32_t, column_t> id_to_global_column;
	for (column_t i = 0; i < global_columns.size(); i++) {
		auto &col = global_columns[i];
		D_ASSERT(!col.identifier.IsNull());
		id_to_global_column[col.identifier.GetValue<int32_t>()] = i;
	}

	unordered_map<idx_t, idx_t> global_id_to_result_id;
	for (idx_t i = 0; i < column_indexes.size(); i++) {
		auto &column_index = column_indexes[i];
		if (column_index.IsVirtualColumn()) {
			continue;
		}
		auto global_id = column_index.GetPrimaryIndex();
		global_id_to_result_id[global_id] = i;
	}
	//! For the column(s) that are needed but aren't referenced, add them to the map
	for (auto field_id : entry.equality_ids) {
		auto global_column_id = id_to_global_column[field_id];
		ColumnIndex equality_index(global_column_id);
		//! Check if the column needed by the equality delete is present
		if (std::find(column_indexes.begin(), column_indexes.end(), equality_index) != column_indexes.end()) {
			continue;
		}
		auto new_result_id = column_indexes.size() + equality_id_to_result_id.size();
		//! Create or get the result id mapping for this equality id
		auto result_id = equality_id_to_result_id.emplace(field_id, new_result_id).first->second;
		global_id_to_result_id[global_column_id] = result_id;
	}

	//! Take only the relevant columns from the result
	InitializeFromOtherChunk(result, result_p, column_ids);
	result.ReferenceColumns(result_p, column_ids);
	deletes.files.emplace_back(entry.partition_values, entry.partition_spec_id);
	auto &rows = deletes.files.back().rows;
	rows.resize(count);
	D_ASSERT(result.ColumnCount() == entry.equality_ids.size());
	for (idx_t col_idx = 0; col_idx < result.ColumnCount(); col_idx++) {
		auto &field_id = entry.equality_ids[col_idx];
		auto global_column_id = id_to_global_column[field_id];
		auto &col = global_columns[global_column_id];
		auto &vec = result.data[col_idx];

		auto it = global_id_to_result_id.find(global_column_id);
		D_ASSERT(it != global_id_to_result_id.end());
		auto result_column_id = it->second;

		for (idx_t i = 0; i < count; i++) {
			auto &row = rows[i];
			auto constant = vec.GetValue(i);
			unique_ptr<Expression> equality_filter;
			auto bound_ref = make_uniq<BoundReferenceExpression>(col.type, result_column_id);
			if (!constant.IsNull()) {
				//! Create a COMPARE_NOT_EQUAL expression
				equality_filter =
				    make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_NOTEQUAL, std::move(bound_ref),
				                                         make_uniq<BoundConstantExpression>(constant));
			} else {
				//! Construct an OPERATOR_IS_NOT_NULL expression instead
				auto is_not_null =
				    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
				is_not_null->children.push_back(std::move(bound_ref));
				equality_filter = std::move(is_not_null);
			}
			row.filters.emplace(std::make_pair(field_id, std::move(equality_filter)));
		}
	}
}

} // namespace duckdb
