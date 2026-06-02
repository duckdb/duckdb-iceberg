#include "core/deletes/iceberg_equality_delete.hpp"

#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

#include "planning/iceberg_multi_file_list.hpp"

namespace duckdb {

static void InitializeFromOtherChunk(DataChunk &target, DataChunk &other, const vector<column_t> &column_ids) {
	vector<LogicalType> types;
	for (auto &id : column_ids) {
		types.push_back(other.data[id].GetType());
	}
	target.InitializeEmpty(types);
}

static void ColumnsReferencedByEqualityIds(DataChunk &source, DataChunk &result,
                                           const vector<MultiFileColumnDefinition> &local_columns,
                                           const vector<int32_t> &equality_ids) {
	//! Map from column_id to 'local_columns' index, to figure out which columns from the 'source' are relevant here
	// 'local_columns' are columns from the equality delete file.
	// id_to_column -> equality_delete_column_field_id_to_output_column_id
	unordered_map<int32_t, column_t> id_to_column;
	for (column_t i = 0; i < local_columns.size(); i++) {
		auto &col = local_columns[i];
		D_ASSERT(!col.identifier.IsNull());
		id_to_column[col.identifier.GetValue<int32_t>()] = i;
	}

	// column_ids we want to slice.
	vector<column_t> column_ids;
	for (auto id : equality_ids) {
		D_ASSERT(id_to_column.count(id));
		column_ids.push_back(id_to_column[id]);
	}
	//! Take only the relevant columns from the source (equality_delete_file)
	InitializeFromOtherChunk(result, source, column_ids);
	result.ReferenceColumns(source, column_ids);
}

void IcebergMultiFileList::ScanEqualityDeleteFile(const BoundIcebergManifestEntry &bound_manifest_entry,
                                                  DataChunk &source, vector<MultiFileColumnDefinition> &local_columns,
                                                  const vector<MultiFileColumnDefinition> &global_columns,
                                                  const vector<ColumnIndex> &global_column_ids,
                                                  const vector<idx_t> &projection_ids) const {
	auto &manifest_entry = bound_manifest_entry.entry;
	auto &data_file = manifest_entry.data_file;
	auto &manifest_file = GetManifestFileForEntry(bound_manifest_entry, IcebergManifestContentType::DELETE);
	D_ASSERT(!data_file.equality_ids.empty());
	D_ASSERT(source.ColumnCount() == local_columns.size());

	auto count = source.size();
	if (count == 0) {
		return;
	}

	// make result only reference the columns from source (equality delete file) that have equality_ids
	// mentioned in the manifest file
	DataChunk result;
	ColumnsReferencedByEqualityIds(source, result, local_columns, data_file.equality_ids);

	const auto sequence_number = manifest_entry.GetSequenceNumber(manifest_file);
	//! Get or create the equality delete data for this sequence number
	auto it = equality_delete_data.find(sequence_number);
	if (it == equality_delete_data.end()) {
		it = equality_delete_data.emplace(sequence_number, make_uniq<IcebergEqualityDeleteData>(sequence_number)).first;
	}
	auto &deletes = *it->second;

	// We are scanning the delete file even before the optimizer runs
	// All equality delete columns will be projected from the scan due to our optimizer
	// we want to know where in the output the equality delete columns will be projected
	unordered_map<idx_t, idx_t> global_id_to_projection_index;
	for (idx_t result_id = 0; result_id < global_column_ids.size(); result_id++) {
		auto global_col = global_column_ids[result_id];
		if (IsVirtualColumn(global_col.GetPrimaryIndex())) {
			continue;
		}
		D_ASSERT(global_col.GetPrimaryIndex() < global_columns.size());
		// index_in_global_columns = index in input_chunk
		auto index_in_global_columns = global_col.GetPrimaryIndex();
		auto &col = global_columns[index_in_global_columns];
		for (auto &equality_delete_col : local_columns) {
			if (equality_delete_col.GetIdentifierFieldId() == col.GetIdentifierFieldId()) {
				if (projection_ids.empty()) {
					global_id_to_projection_index[index_in_global_columns] = result_id;
				} else {
					for (idx_t proj_index = 0; proj_index < projection_ids.size(); proj_index++) {
						idx_t projection_col = projection_ids[proj_index];
						if (projection_col == result_id) {
							global_id_to_projection_index[index_in_global_columns] = proj_index;
						}
					}
				}
				// here we can break. col has one identifier field id and equality deletes should only have unique
				// values
				break;
			}
		}
	}

	unordered_map<int32_t, column_t> field_id_to_global_column;
	for (idx_t i = 0; i < global_columns.size(); i++) {
		auto &global_col = global_columns.at(i);
		field_id_to_global_column[global_col.GetIdentifierFieldId()] = i;
	}

	deletes.files.emplace_back(data_file.partition_info, manifest_file.partition_spec_id);
	auto &rows = deletes.files.back().rows;
	rows.resize(count);
	D_ASSERT(result.ColumnCount() == data_file.equality_ids.size());

	for (idx_t col_idx = 0; col_idx < result.ColumnCount(); col_idx++) {
		auto &field_id = data_file.equality_ids[col_idx];
		auto global_column_id = field_id_to_global_column[field_id];
		auto &col = global_columns[global_column_id];
		auto &vec = result.data[col_idx];

		auto it = global_id_to_projection_index.find(global_column_id);
		D_ASSERT(it != global_id_to_projection_index.end());
		auto result_column_id = it->second;

		for (idx_t i = 0; i < count; i++) {
			auto &row = rows[i];
			auto constant = vec.GetValue(i);

			unique_ptr<Expression> equality_filter;
			// this bound ref is on the position of the output_chunk data.
			auto bound_ref = make_uniq<BoundReferenceExpression>(col.type, result_column_id);
			if (!constant.IsNull()) {
				//! Create a COMPARE_NOT_EQUAL expression
				equality_filter =
				    BoundComparisonExpression::Create(ExpressionType::COMPARE_NOTEQUAL, std::move(bound_ref),
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
