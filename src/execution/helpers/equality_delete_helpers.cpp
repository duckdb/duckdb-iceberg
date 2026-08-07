#include "execution/operator/iceberg_delete.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "common/iceberg_utils.hpp"
#include "core/expression/iceberg_value.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "iceberg_options.hpp"

namespace duckdb {

//! Equality-delete write helpers. The functions defined here are only invoked when the
//! ICEBERG_ENABLE_EQUALITY_DELETE_WRITES compile flag is on - in default builds the callers
//! (in iceberg_delete.cpp) are #ifdef'd out, so this code is dead.

//! Whether a physical-filter expression is built purely from equality-delete forms, i.e. `col = const`,
//! `col IN (const, ...)`, `col IS NULL`, and AND/OR of those. `col IN (...)` and `col = c1 OR ...` can leave such a
//! physical filter behind even though they also push down as a scan filter; recognizing it here keeps that delete on
//! the equality-delete path. Anything else (ranges, functions, arbitrary expressions) disqualifies.
static bool ExpressionIsEqualityDeleteForm(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
	    expr.GetExpressionType() == ExpressionType::COMPARE_EQUAL) {
		auto &comparison = expr.Cast<BoundFunctionExpression>();
		auto &left = BoundComparisonExpression::Left(comparison);
		auto &right = BoundComparisonExpression::Right(comparison);
		//! Exactly one side must be a constant (the other is the column expression).
		return (left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) !=
		       (right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT);
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
	    expr.GetExpressionType() == ExpressionType::COMPARE_IN) {
		auto &op = expr.Cast<BoundOperatorExpression>();
		auto &children = op.GetChildren();
		if (children.size() < 2 || children[0]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			return false;
		}
		for (idx_t i = 1; i < children.size(); i++) {
			if (children[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
				return false;
			}
		}
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
	    expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL) {
		auto &children = expr.Cast<BoundOperatorExpression>().GetChildren();
		return children.size() == 1 && children[0]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conjunction.GetChildren()) {
			if (!ExpressionIsEqualityDeleteForm(*child)) {
				return false;
			}
		}
		return true;
	}
	return false;
}

static bool PlanContainsPhysicalFilter(PhysicalOperator &plan) {
	if (plan.type == PhysicalOperatorType::FILTER) {
		auto &filter = plan.Cast<PhysicalFilter>();
		//! The DELETE predicate can leave a physical filter behind even when it is a pure equality form
		//! (`col IN (...)` / `col = c1 OR ...` also push down as a scan filter). Such a filter must not
		//! disqualify writing an equality delete; anything else does.
		if (!ExpressionIsEqualityDeleteForm(*filter.expression)) {
			return true;
		}
	}
	for (auto &child : plan.children) {
		if (PlanContainsPhysicalFilter(child.get())) {
			return true;
		}
	}
	return false;
}

namespace {

static bool TryGetColumnPath(const Expression &expr, vector<Identifier> &column_path);

static bool TryGetFunctionColumnPath(const BoundFunctionExpression &function, vector<Identifier> &column_path) {
	idx_t child_index;
	if (!TryGetStructExtractChildIndex(function, child_index) || function.GetChildren().empty()) {
		return false;
	}
	auto &base = *function.GetChildren()[0];
	if (!TryGetColumnPath(base, column_path) || base.GetReturnType().id() != LogicalTypeId::STRUCT ||
	    child_index >= StructType::GetChildCount(base.GetReturnType())) {
		return false;
	}
	//! Recurse into the base first so nested extracts produce a root-to-leaf field path.
	column_path.push_back(StructType::GetChildName(base.GetReturnType(), child_index));
	return true;
}

static bool TryGetOperatorColumnPath(const BoundOperatorExpression &struct_extract, vector<Identifier> &column_path) {
	auto &children = struct_extract.GetChildren();
	if (children.size() != 2 || children[1]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		return false;
	}
	auto &field_value = children[1]->Cast<BoundConstantExpression>().GetValue();
	if (field_value.IsNull() || field_value.type().id() != LogicalTypeId::VARCHAR ||
	    !TryGetColumnPath(*children[0], column_path)) {
		return false;
	}

	auto remaining_fields = QualifiedName::ParseComponents(field_value.GetValue<string>());
	//! A STRUCT_EXTRACT operator can encode more than one remaining field in its constant.
	for (auto &field : remaining_fields) {
		column_path.push_back(std::move(field));
	}
	return !remaining_fields.empty();
}

static bool TryGetColumnPath(const Expression &expr, vector<Identifier> &column_path) {
	switch (expr.GetExpressionType()) {
	case ExpressionType::BOUND_REF:
	case ExpressionType::BOUND_COLUMN_REF:
		return true;
	case ExpressionType::STRUCT_EXTRACT:
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
			return TryGetOperatorColumnPath(expr.Cast<BoundOperatorExpression>(), column_path);
		}
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			return TryGetFunctionColumnPath(expr.Cast<BoundFunctionExpression>(), column_path);
		}
		return false;
	case ExpressionType::BOUND_FUNCTION:
		return TryGetFunctionColumnPath(expr.Cast<BoundFunctionExpression>(), column_path);
	default:
		return false;
	}
}

static bool SetOrMatchColumnExpression(optional_ptr<const Expression> &column_expression, const Expression &candidate) {
	if (!column_expression) {
		column_expression = candidate;
		return true;
	}
	//! Every disjunct of an OR must reference the same column.
	return Expression::Equals(*column_expression, candidate);
}

//! Extract the (single) column expression a filter targets and the set of values it deletes, for the
//! supported forms: `col = c`, `col IN (c1, ...)`, `col IS NULL`, and `col = c1 OR col = c2 OR ...` (which may
//! nest IN / IS NULL). NULL constants in equality and IN expressions are dropped because those predicates do
//! not match NULL; only IS NULL contributes a NULL equality-delete key. Returns false for any other shape.
static bool TryGetEqualityDeleteValuesFromExpression(const Expression &expr,
                                                     optional_ptr<const Expression> &column_expression,
                                                     vector<Value> &values) {
	//! `col IN (...)` / `col = c1 OR ...` push down as an optional scan filter, wrapped in an
	//! optional-filter scalar function whose real predicate lives in the bind info. Unwrap it.
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.Function().GetName() == OptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<OptionalFilterFunctionData>();
			return data.child_filter_expr &&
			       TryGetEqualityDeleteValuesFromExpression(*data.child_filter_expr, column_expression, values);
		}
		if (func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			return data.child_filter_expr &&
			       TryGetEqualityDeleteValuesFromExpression(*data.child_filter_expr, column_expression, values);
		}
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
	    expr.GetExpressionType() == ExpressionType::COMPARE_EQUAL) {
		auto &compare_expr = expr.Cast<BoundFunctionExpression>();
		auto &left = BoundComparisonExpression::Left(compare_expr);
		auto &right = BoundComparisonExpression::Right(compare_expr);
		optional_ptr<const Expression> column;
		optional_ptr<const Value> constant;
		if (right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			column = left;
			constant = right.Cast<BoundConstantExpression>().GetValue();
		} else if (left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			column = right;
			constant = left.Cast<BoundConstantExpression>().GetValue();
		} else {
			return false;
		}
		if (!SetOrMatchColumnExpression(column_expression, *column)) {
			return false;
		}
		if (!constant->IsNull()) {
			values.push_back(*constant);
		}
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
	    expr.GetExpressionType() == ExpressionType::COMPARE_IN) {
		auto &op = expr.Cast<BoundOperatorExpression>();
		auto &children = op.GetChildren();
		if (children.size() < 2 || !SetOrMatchColumnExpression(column_expression, *children[0])) {
			return false;
		}
		for (idx_t i = 1; i < children.size(); i++) {
			if (children[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
				return false;
			}
			auto &value = children[i]->Cast<BoundConstantExpression>().GetValue();
			if (!value.IsNull()) {
				values.push_back(value);
			}
		}
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
	    expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL) {
		auto &children = expr.Cast<BoundOperatorExpression>().GetChildren();
		if (children.size() != 1 || !SetOrMatchColumnExpression(column_expression, *children[0])) {
			return false;
		}
		values.emplace_back(children[0]->GetReturnType());
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conjunction.GetChildren()) {
			if (!TryGetEqualityDeleteValuesFromExpression(*child, column_expression, values)) {
				return false;
			}
		}
		return !conjunction.GetChildren().empty();
	}
	return false;
}

} // namespace

bool IcebergDelete::TryGetEqualityDeletePredicates(ClientContext &context, IcebergTableSchemaVersion &table,
                                                   PhysicalOperator &child_plan,
                                                   vector<IcebergEqualityDeletePredicate> &equality_predicates) {
	//! Gated behind an explicit testing-only setting.
	Value setting_value;
	if (!context.TryGetCurrentSetting(ENABLE_EQUALITY_DELETES_CONFIG_VARIABLE, setting_value) ||
	    setting_value.IsNull() || !setting_value.GetValue<bool>()) {
		return false;
	}

	//! Equality-delete writing is only supported for v2, unpartitioned tables.
	auto &table_metadata = table.table_info.table_metadata;
	if (table_metadata.iceberg_version != 2) {
		return false;
	}
	if (table_metadata.HasPartitionSpec() && table_metadata.GetLatestPartitionSpec().IsPartitioned()) {
		return false;
	}

	//! Any filter means this cannot be an equality delete.
	if (PlanContainsPhysicalFilter(child_plan)) {
		return false;
	}

	auto table_scan = FindIcebergScan(child_plan);
	if (!table_scan) {
		return false;
	}
	auto &scan = *table_scan;
	if (!scan.table_filters || !scan.table_filters->HasFilters()) {
		return false;
	}

	auto &schema = table_metadata.GetLatestSchema();
	auto &columns = schema.columns;
	for (auto &filter_entry : *scan.table_filters) {
		auto column_key = filter_entry.GetIndex().GetIndex();
		auto &table_filter = filter_entry.Filter().Cast<ExpressionFilter>();
		auto &expr = *table_filter.expr;

		//! Accept equality predicates, IN lists, and IS NULL (including OR combinations); reject anything else.
		optional_ptr<const Expression> column_expression;
		vector<Value> raw_values;
		if (!TryGetEqualityDeleteValuesFromExpression(expr, column_expression, raw_values)) {
			return false;
		}
		if (!column_expression || raw_values.empty()) {
			//! e.g. `col IN (NULL)` - nothing to delete via equality; fall back to positional.
			return false;
		}
		vector<Identifier> column_path;
		if (!TryGetColumnPath(*column_expression, column_path)) {
			return false;
		}

		if (column_key >= scan.column_ids.size()) {
			return false;
		}
		auto &column_index = scan.column_ids[column_key];
		if (column_index.IsVirtualColumn()) {
			return false;
		}
		auto primary_index = column_index.GetPrimaryIndex();
		if (primary_index >= columns.size()) {
			return false;
		}
		optional_ptr<const IcebergColumnDefinition> column_definition = columns[primary_index].get();
		for (auto &child_name : column_path) {
			column_definition = column_definition->GetChild(child_name.GetIdentifierName());
			if (!column_definition) {
				return false;
			}
		}
		//! The same column referenced more than once is not a clean equality delete.
		for (auto &existing : equality_predicates) {
			if (existing.field_id == column_definition->id) {
				return false;
			}
		}
		IcebergEqualityDeletePredicate predicate;
		predicate.field_id = column_definition->id;
		predicate.column_name = column_definition->name;
		predicate.type = column_definition->type;
		for (auto &raw_value : raw_values) {
			Value delete_value;
			if (raw_value.IsNull()) {
				delete_value = Value(column_definition->type);
			} else {
				string error_message;
				if (!raw_value.DefaultTryCastAs(column_definition->type, delete_value, &error_message, true)) {
					return false;
				}
			}
			predicate.values.push_back(std::move(delete_value));
		}
		equality_predicates.push_back(std::move(predicate));
	}

	if (equality_predicates.empty()) {
		return false;
	}
	//! The equality-delete file materializes the cross product of every column's value set. Cap it so a
	//! very large delete falls back to positional deletes instead of writing a huge equality-delete file.
	static constexpr idx_t MAX_EQUALITY_DELETE_ROWS = 4096;
	idx_t total_rows = 1;
	for (auto &predicate : equality_predicates) {
		total_rows *= predicate.values.size();
		if (total_rows > MAX_EQUALITY_DELETE_ROWS) {
			return false;
		}
	}
	return true;
}

//! This writer gives an equality delete no partition values, so only an unpartitioned spec makes it apply at
//! all; a partitioned one would scope it to a partition it never names. Lowest id, to keep the choice stable.
static int32_t GlobalEqualityDeleteSpecId(const IcebergTableMetadata &metadata) {
	int32_t result = 0;
	bool found = false;
	for (auto &entry : metadata.GetPartitionSpecs()) {
		if (!entry.second.IsUnpartitioned()) {
			continue;
		}
		if (!found || entry.first < result) {
			result = entry.first;
			found = true;
		}
	}
	if (!found) {
		throw NotImplementedException(
		    "Cannot write an equality delete for this table: none of its partition specs are unpartitioned, and "
		    "writing partition-scoped equality deletes is not supported");
	}
	return result;
}

void IcebergDelete::WriteEqualityDeleteFile(ClientContext &context, IcebergDeleteGlobalState &global_state) const {
	D_ASSERT(!equality_predicates.empty());

	auto &fs = FileSystem::GetFileSystem(context);
	auto data_path = table.table_info.table_metadata.GetDataPath(fs);
	string delete_filename = UUID::ToString(UUID::GenerateRandomUUID()) + "-equality-deletes.parquet";
	string delete_file_path = fs.JoinPath(data_path, delete_filename);

	auto info = make_uniq<CopyInfo>();
	info->file_path = delete_file_path;
	info->format = "parquet";
	info->is_from = false;

	// Generate the field ids for the parquet writer: every column carries, as PARQUET:field_id
	// metadata, the iceberg field-id that the equality delete applies to.
	child_list_t<Value> field_id_values;
	vector<string> names_to_write;
	vector<LogicalType> types_to_write;
	vector<int32_t> equality_ids;
	for (auto &predicate : equality_predicates) {
		field_id_values.emplace_back(predicate.column_name, Value::INTEGER(predicate.field_id));
		names_to_write.push_back(predicate.column_name);
		types_to_write.push_back(predicate.type);
		equality_ids.push_back(predicate.field_id);
	}
	vector<Value> field_input;
	field_input.push_back(Value::STRUCT(std::move(field_id_values)));
	info->options["field_ids"] = std::move(field_input);

	auto &copy_fun = IcebergUtils::GetCopyFunction(context, "parquet");
	CopyFunctionBindInput bind_input(*info);

	auto function_data =
	    copy_fun.function.copy_to_bind(context, bind_input, StringsToIdentifiers(names_to_write), types_to_write);
	auto copy_global_state = copy_fun.function.copy_to_initialize_global(context, *function_data, delete_file_path);

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto copy_local_state = copy_fun.function.copy_to_initialize_local(execution_context, *function_data);

	CopyFunctionFileStatistics stats;
	copy_fun.function.copy_to_get_written_statistics(context, *function_data, *copy_global_state, stats);

	// Materialize the equality-delete rows: the cross product of every column's value set. Within a row
	// the columns are AND-ed and rows are OR-ed, encoding `(col0 IN vals0) AND (col1 IN vals1) AND ...`.
	vector<vector<Value>> rows;
	rows.emplace_back();
	for (auto &predicate : equality_predicates) {
		vector<vector<Value>> expanded;
		for (auto &existing_row : rows) {
			for (auto &value : predicate.values) {
				auto new_row = existing_row;
				new_row.push_back(value);
				expanded.push_back(std::move(new_row));
			}
		}
		rows = std::move(expanded);
	}

	// Write the delete tuples (one per row), chunking at STANDARD_VECTOR_SIZE.
	idx_t rows_written = 0;
	while (rows_written < rows.size()) {
		idx_t chunk_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, rows.size() - rows_written);
		DataChunk write_chunk;
		write_chunk.Initialize(context, types_to_write);
		for (idx_t row_idx = 0; row_idx < chunk_count; row_idx++) {
			auto &row = rows[rows_written + row_idx];
			for (idx_t col_idx = 0; col_idx < row.size(); col_idx++) {
				write_chunk.data[col_idx].SetValue(row_idx, row[col_idx]);
			}
		}
		write_chunk.SetChildCardinality(chunk_count);
		copy_fun.function.copy_to_sink(execution_context, *function_data, *copy_global_state, *copy_local_state,
		                               write_chunk);
		rows_written += chunk_count;
	}

	copy_fun.function.copy_to_combine(execution_context, *function_data, *copy_global_state, *copy_local_state);
	copy_fun.function.copy_to_finalize(context, *function_data, *copy_global_state);

	IcebergDeleteFileInfo delete_file;
	delete_file.file_name = delete_file_path;
	delete_file.file_format = "parquet";
	delete_file.delete_count = rows.size();
	delete_file.file_size_bytes = stats.file_size_bytes;
	delete_file.equality_ids = std::move(equality_ids);
	//! An equality delete targets no single data file, so it is not tied to any one partition.
	delete_file.partition_info.partition_spec_id = GlobalEqualityDeleteSpecId(table.table_info.table_metadata);

	// Record per-field metrics for the equality-delete values so scans can prune this delete file when its
	// equality-field range is disjoint from the scan predicate / a data file's bounds. Bounds span the min/max
	// of the non-null, non-NaN values because Iceberg bounds exclude nulls and NaNs.
	for (auto &predicate : equality_predicates) {
		optional<Value> min_value;
		optional<Value> max_value;
		bool has_null = false;
		bool has_nan = false;
		for (auto &value : predicate.values) {
			if (value.IsNull()) {
				has_null = true;
				continue;
			}
			bool is_nan = false;
			if (predicate.type.id() == LogicalTypeId::FLOAT) {
				is_nan = Value::IsNan(value.GetValue<float>());
			} else if (predicate.type.id() == LogicalTypeId::DOUBLE) {
				is_nan = Value::IsNan(value.GetValue<double>());
			}
			if (is_nan) {
				has_nan = true;
				continue;
			}
			if (!min_value || value < *min_value) {
				min_value = value;
			}
			if (!max_value || value > *max_value) {
				max_value = value;
			}
		}
		if (!has_null) {
			delete_file.null_value_counts[predicate.field_id] = 0;
		}
		if (predicate.type.id() == LogicalTypeId::FLOAT || predicate.type.id() == LogicalTypeId::DOUBLE) {
			if (!has_nan) {
				delete_file.nan_value_counts[predicate.field_id] = 0;
			}
		}
		if (!min_value || !max_value) {
			continue;
		}
		auto lower = IcebergValue::SerializeValue(*min_value, predicate.type, SerializeBound::LOWER_BOUND);
		if (lower.HasError()) {
			throw InvalidConfigurationException(lower.GetError());
		} else if (lower.HasValue()) {
			delete_file.lower_bounds[predicate.field_id] = lower.GetValue();
		}
		auto upper = IcebergValue::SerializeValue(*max_value, predicate.type, SerializeBound::UPPER_BOUND);
		if (upper.HasError()) {
			throw InvalidConfigurationException(upper.GetError());
		} else if (upper.HasValue()) {
			delete_file.upper_bounds[predicate.field_id] = upper.GetValue();
		}
	}

	global_state.written_files.emplace(delete_file_path, std::move(delete_file));
}

} // namespace duckdb
