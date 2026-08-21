#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/binder.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "function/iceberg_functions.hpp"
#include "maintenance/rewrite_data_files_operator.hpp"
#include "maintenance/rewrite_data_files_planner.hpp"

namespace duckdb {

namespace {

constexpr int64_t MIN_TARGET_FILE_SIZE_BYTES = 100;

static QualifiedName ParseRewriteTableName(const string &identifier) {
	auto parts = QualifiedName::ParseComponents(identifier);
	if (parts.size() != 3) {
		throw InvalidInputException(
		    "iceberg_rewrite_data_files: table identifier must be 'catalog.schema.table', got '%s'", identifier);
	}
	for (auto &part : parts) {
		if (part.empty()) {
			throw InvalidInputException("iceberg_rewrite_data_files: table identifier '%s' has an empty component",
			                            identifier);
		}
	}
	return QualifiedName {parts[0], parts[1], parts[2]};
}

static int64_t ParseByteSizeNamedParameter(const Value &value, const char *parameter_name) {
	idx_t parsed_value;
	if (value.type().id() == LogicalTypeId::VARCHAR) {
		auto error = StringUtil::TryParseFormattedBytes(StringValue::Get(value), parsed_value);
		if (!error.empty()) {
			throw InvalidInputException("iceberg_rewrite_data_files: invalid '%s': %s", parameter_name, error);
		}
	} else {
		auto numeric_value = value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
		if (numeric_value < MIN_TARGET_FILE_SIZE_BYTES) {
			throw InvalidInputException("iceberg_rewrite_data_files: '%s' must be >= %lld bytes, got %lld",
			                            parameter_name, MIN_TARGET_FILE_SIZE_BYTES, numeric_value);
		}
		return numeric_value;
	}
	if (parsed_value < static_cast<idx_t>(MIN_TARGET_FILE_SIZE_BYTES)) {
		throw InvalidInputException("iceberg_rewrite_data_files: '%s' must be >= %lld bytes, got %llu", parameter_name,
		                            MIN_TARGET_FILE_SIZE_BYTES, static_cast<unsigned long long>(parsed_value));
	}
	if (parsed_value > static_cast<idx_t>(NumericLimits<int64_t>::Maximum())) {
		throw InvalidInputException("iceberg_rewrite_data_files: '%s' is too large", parameter_name);
	}
	return static_cast<int64_t>(parsed_value);
}

static RewriteDataFilesPlanInput ParseRewritePlanInput(TableFunctionBindInput &input) {
	RewriteDataFilesPlanInput result;
	result.table_name = ParseRewriteTableName(StringValue::Get(input.inputs[0]));

	for (auto &kv : input.named_parameters) {
		auto opt = StringUtil::Lower(kv.first.GetIdentifierName());
		auto &val = kv.second;
		if (opt == "target_file_size_bytes") {
			result.target_file_size_bytes = ParseByteSizeNamedParameter(val, "target_file_size_bytes");
		} else if (opt == "min_file_size_bytes") {
			result.min_file_size_bytes = ParseByteSizeNamedParameter(val, "min_file_size_bytes");
		} else if (opt == "max_file_size_bytes") {
			result.max_file_size_bytes = ParseByteSizeNamedParameter(val, "max_file_size_bytes");
		} else if (opt == "min_input_files") {
			auto value = val.GetValue<int64_t>();
			if (value < 1) {
				throw InvalidInputException("iceberg_rewrite_data_files: 'min_input_files' must be >= 1, got %lld",
				                            value);
			}
			result.min_input_files = value;
		} else if (opt == "rewrite_all") {
			result.rewrite_all = BooleanValue::Get(val);
		}
	}
	if (result.min_file_size_bytes && result.max_file_size_bytes &&
	    result.min_file_size_bytes.value() > result.max_file_size_bytes.value()) {
		throw InvalidInputException(
		    "iceberg_rewrite_data_files: 'min_file_size_bytes' (%lld) must be <= 'max_file_size_bytes' (%lld)",
		    result.min_file_size_bytes.value(), result.max_file_size_bytes.value());
	}
	return result;
}

static unique_ptr<QueryNode> BuildCandidateSelect(const QualifiedName &table_name,
                                                  const vector<RewriteCandidate> &candidates) {
	auto select = make_uniq<SelectNode>();
	select->select_list.push_back(make_uniq<StarExpression>());

	auto table = make_uniq<BaseTableRef>();
	table->SetQualifiedName(table_name);
	table->alias = "rewrite_source";
	select->from_table = std::move(table);

	vector<unique_ptr<ParsedExpression>> in_children;
	in_children.push_back(make_uniq<ColumnRefExpression>("filename"));
	for (auto &candidate : candidates) {
		in_children.push_back(make_uniq<ConstantExpression>(Value(candidate.file_path)));
	}
	//! Read through the attached Iceberg table so the scan layer applies MoR
	//! position/equality deletes, then scope the scan to the selected rewrite
	//! source files using the `filename` virtual column.
	select->where_clause = make_uniq<OperatorExpression>(ExpressionType::COMPARE_IN, std::move(in_children));
	return std::move(select);
}

//! Bind a LogicalCopyToFile over the candidate scan. The logical COPY is not
//! executed as-is: CreatePlan peels it off and rebuilds a PhysicalCopyToFile via
//! IcebergInsert::PlanCopyForInsert (partitioned IcebergCopyOptions). Binding
//! COPY still matters because RemoveUnusedColumns treats LOGICAL_COPY_TO_FILE as
//! "everything referenced", preserving all table columns for the later physical
//! rewrite copy.
static unique_ptr<LogicalOperator> BindCandidateCopy(Binder &binder, const RewritePlan &plan) {
	auto &metadata = plan.table_info->table_metadata;
	auto schema_id = metadata.GetCurrentSchemaId();
	auto schema_it = metadata.GetSchemas().find(schema_id);
	if (schema_it == metadata.GetSchemas().end()) {
		throw InternalException("iceberg_rewrite_data_files: current schema id %d not found in metadata", schema_id);
	}

	auto &fs = FileSystem::GetFileSystem(binder.context);
	CopyStatement copy_statement;
	copy_statement.info->select_statement = BuildCandidateSelect(plan.table_name, plan.selected_candidates);
	copy_statement.info->file_path = metadata.GetDataPath(fs);
	copy_statement.info->format = "parquet";
	copy_statement.info->is_from = false;
	copy_statement.info->is_format_auto_detected = false;
	copy_statement.info->options["field_ids"].push_back(schema_it->second->GetFieldIds());
	copy_statement.info->options["filename_pattern"].push_back(Value("{uuidv7}"));
	copy_statement.info->options["file_size_bytes"].push_back(
	    Value::UBIGINT(static_cast<uint64_t>(plan.target_file_size_bytes)));
	copy_statement.info->options["return_stats"].push_back(Value::BOOLEAN(true));
	copy_statement.info->options["per_thread_output"].push_back(Value::BOOLEAN(false));
	copy_statement.info->options["append"].push_back(Value::BOOLEAN(true));

	auto copy_binder = Binder::CreateBinder(binder.context, &binder);
	auto bound_copy = copy_binder->Bind(copy_statement);
	return std::move(bound_copy.plan);
}

static unique_ptr<LogicalOperator> RewriteDataFilesBindOperator(ClientContext &context, TableFunctionBindInput &input,
                                                                TableIndex bind_index,
                                                                vector<Identifier> &return_names) {
	if (!input.binder) {
		throw InternalException("iceberg_rewrite_data_files: bind_operator called without a binder");
	}
	auto plan_input = ParseRewritePlanInput(input);
	input.binder->SetAlwaysRequireRebind();
	auto plan = PlanRewrite(context, plan_input);

	auto result = make_uniq<LogicalRewriteDataFiles>(bind_index.index, std::move(plan));
	if (!result->plan.selected_candidates.empty()) {
		result->children.push_back(BindCandidateCopy(*input.binder, result->plan));
	}

	return_names = {"rewritten_data_files", "added_data_files", "rewritten_bytes"};
	return std::move(result);
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergRewriteDataFilesFunction() {
	TableFunctionSet function_set("iceberg_rewrite_data_files");
	TableFunction function("iceberg_rewrite_data_files", {LogicalType::VARCHAR}, nullptr);
	function.bind_operator = RewriteDataFilesBindOperator;
	function.named_parameters["target_file_size_bytes"] = LogicalType::ANY;
	function.named_parameters["min_file_size_bytes"] = LogicalType::ANY;
	function.named_parameters["max_file_size_bytes"] = LogicalType::ANY;
	function.named_parameters["min_input_files"] = LogicalType::BIGINT;
	function.named_parameters["rewrite_all"] = LogicalType::BOOLEAN;
	function_set.AddFunction(function);
	return function_set;
}

} // namespace duckdb
