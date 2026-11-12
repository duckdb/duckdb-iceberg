#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "function/iceberg_functions.hpp"
#include "maintenance/rewrite_data_files_operator.hpp"
#include "maintenance/rewrite_data_files_planner.hpp"

namespace duckdb {

namespace {

constexpr int64_t MIN_TARGET_FILE_SIZE_BYTES = 100;

static QualifiedName ParseRewriteTableName(ClientContext &context, string identifier) {
	auto name = IcebergUtils::ParseTableName(identifier);
	if (name.Path().size() == 3) {
		return name;
	}
	//! The plan reloads the table by name when it runs, so keep the name it resolved to now
	auto entry = IcebergUtils::GetTableEntry(context, identifier);
	if (entry->Cast<InCatalogEntry>().catalog.GetCatalogType() != "iceberg") {
		throw InvalidInputException("iceberg_rewrite_data_files: '%s' is not an Iceberg table", identifier);
	}
	return IcebergUtils::GetQualifiedTableName(*entry);
}

static int64_t ParseByteSizeNamedParameter(const Value &value, const char *parameter_name) {
	if (value.type().id() == LogicalTypeId::VARCHAR) {
		idx_t parsed_value;
		auto error = StringUtil::TryParseFormattedBytes(StringValue::Get(value), parsed_value);
		if (!error.empty()) {
			throw InvalidInputException("iceberg_rewrite_data_files: invalid '%s': %s", parameter_name, error);
		}
		if (parsed_value > static_cast<idx_t>(NumericLimits<int64_t>::Maximum())) {
			throw InvalidInputException("iceberg_rewrite_data_files: '%s' is too large", parameter_name);
		}
		return static_cast<int64_t>(parsed_value);
	}
	return value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
}

static RewriteDataFilesPlanInput ParseRewritePlanInput(ClientContext &context, TableFunctionBindInput &input) {
	RewriteDataFilesPlanInput result;
	result.table_name = ParseRewriteTableName(context, StringValue::Get(input.inputs[0]));

	for (auto &kv : input.named_parameters) {
		auto opt = StringUtil::Lower(kv.first.GetIdentifierName());
		auto &val = kv.second;
		if (opt == "target_file_size_bytes") {
			auto target = ParseByteSizeNamedParameter(val, "target_file_size_bytes");
			if (target <= 0) {
				throw InvalidInputException(
				    "iceberg_rewrite_data_files: 'target_file_size_bytes' must be > 0 bytes, got %lld", target);
			}
			result.target_file_size_bytes = target;
		} else if (opt == "min_file_size_bytes") {
			auto min_size = ParseByteSizeNamedParameter(val, "min_file_size_bytes");
			if (min_size < 0) {
				throw InvalidInputException(
				    "iceberg_rewrite_data_files: 'min_file_size_bytes' must be >= 0 bytes, got %lld", min_size);
			}
			result.min_file_size_bytes = min_size;
		} else if (opt == "max_file_size_bytes") {
			auto max_size = ParseByteSizeNamedParameter(val, "max_file_size_bytes");
			if (max_size <= 0) {
				throw InvalidInputException(
				    "iceberg_rewrite_data_files: 'max_file_size_bytes' must be > 0 bytes, got %lld", max_size);
			}
			result.max_file_size_bytes = max_size;
		} else if (opt == "min_input_files") {
			auto value = val.GetValue<int64_t>();
			if (value < 1) {
				throw InvalidInputException("iceberg_rewrite_data_files: 'min_input_files' must be >= 1, got %lld",
				                            value);
			}
			result.min_input_files = value;
		} else if (opt == "max_files_to_rewrite") {
			if (val.IsNull()) {
				throw InvalidInputException("iceberg_rewrite_data_files: 'max_files_to_rewrite' cannot be NULL");
			}
			auto value = val.GetValue<int64_t>();
			if (value < 1) {
				throw InvalidInputException("iceberg_rewrite_data_files: 'max_files_to_rewrite' must be >= 1, got %lld",
				                            value);
			}
			result.max_files_to_rewrite = value;
		} else if (opt == "rewrite_all") {
			result.rewrite_all = BooleanValue::Get(val);
		} else if (opt == "output_spec_id") {
			if (val.IsNull()) {
				throw InvalidInputException("iceberg_rewrite_data_files: 'output_spec_id' cannot be NULL");
			}
			result.output_spec_id = val.GetValue<int32_t>();
		}
	}
	if (result.min_file_size_bytes && result.max_file_size_bytes &&
	    result.min_file_size_bytes.value() > result.max_file_size_bytes.value()) {
		throw InvalidInputException(
		    "iceberg_rewrite_data_files: 'min_file_size_bytes' (%lld) must be <= 'max_file_size_bytes' (%lld)",
		    result.min_file_size_bytes.value(), result.max_file_size_bytes.value());
	}
	//! When the caller sets the target, compare it with any explicit threshold. Defaults are 75% and 180%
	//! of the target, so an omitted threshold cannot violate the Spark ordering.
	if (result.target_file_size_bytes) {
		auto target = result.target_file_size_bytes.value();
		auto min_size = result.min_file_size_bytes ? result.min_file_size_bytes.value() : (target * 3) / 4;
		auto max_size = result.max_file_size_bytes ? result.max_file_size_bytes.value() : (target * 9) / 5;
		if (target <= min_size) {
			throw InvalidInputException("iceberg_rewrite_data_files: 'target_file_size_bytes' (%lld) must be > "
			                            "'min_file_size_bytes' (%lld)",
			                            target, min_size);
		}
		if (target >= max_size) {
			throw InvalidInputException("iceberg_rewrite_data_files: 'target_file_size_bytes' (%lld) must be < "
			                            "'max_file_size_bytes' (%lld)",
			                            target, max_size);
		}
	}
	return result;
}

//! Read through the attached Iceberg table so the scan layer applies MoR position/equality deletes.
static unique_ptr<QueryNode> BuildCandidateSelect(const QualifiedName &table_name) {
	auto select = make_uniq<SelectNode>();
	select->select_list.push_back(make_uniq<StarExpression>());

	auto table = make_uniq<BaseTableRef>();
	table->SetQualifiedName(table_name);
	table->alias = "rewrite_source";
	select->from_table = std::move(table);
	return std::move(select);
}

//! Scope the scan under the bound COPY to the selected files with a filter on the `filename` virtual column. The
//! column is added by its id, not its name, so a table column that is also called `filename` can't hide it.
static void FilterCandidateFiles(LogicalOperator &copy, const vector<RewriteCandidate> &candidates) {
	reference<LogicalOperator> parent = copy;
	while (parent.get().children.size() == 1 && parent.get().children[0]->type != LogicalOperatorType::LOGICAL_GET) {
		parent = *parent.get().children[0];
	}
	if (parent.get().children.size() != 1) {
		throw InternalException("iceberg_rewrite_data_files: expected the COPY to read a single table scan");
	}
	auto &get = parent.get().children[0]->Cast<LogicalGet>();
	auto entry = get.virtual_columns.find(MultiFileReader::COLUMN_IDENTIFIER_FILENAME);
	if (entry == get.virtual_columns.end()) {
		throw InternalException("iceberg_rewrite_data_files: the table scan has no filename column");
	}
	auto &column_ids = get.GetMutableColumnIds();
	column_ids.emplace_back(MultiFileReader::COLUMN_IDENTIFIER_FILENAME);
	ColumnBinding binding(get.table_index, ProjectionIndex(column_ids.size() - 1));

	auto in = make_uniq<BoundOperatorExpression>(ExpressionType::COMPARE_IN, LogicalType::BOOLEAN);
	in->GetChildrenMutable().push_back(
	    make_uniq<BoundColumnRefExpression>(entry->second.name, entry->second.type, binding));
	for (auto &candidate : candidates) {
		in->GetChildrenMutable().push_back(make_uniq<BoundConstantExpression>(Value(candidate.file_path)));
	}
	auto filter = make_uniq<LogicalFilter>(std::move(in));
	filter->children.push_back(std::move(parent.get().children[0]));
	parent.get().children[0] = std::move(filter);
}

//! Bind a LogicalCopyToFile over the candidate scan. The logical COPY is not
//! executed as-is: CreatePlan peels it off and rebuilds a PhysicalCopyToFile via
//! IcebergInsert::PlanCopyForInsert (partitioned IcebergCopyOptions). Binding
//! COPY still matters because RemoveUnusedColumns treats LOGICAL_COPY_TO_FILE as
//! "everything referenced", preserving all table columns for the later physical
//! rewrite copy.
static unique_ptr<LogicalOperator> BindCandidateCopy(Binder &binder, const RewritePlan &plan) {
	auto &metadata = plan.table_info->table_metadata;
	auto &current_schema = metadata.GetLatestSchema();

	auto &fs = FileSystem::GetFileSystem(binder.context);
	CopyStatement copy_statement;
	copy_statement.info->select_statement = BuildCandidateSelect(plan.table_name);
	copy_statement.info->file_path = metadata.GetDataPath(fs);
	copy_statement.info->format = "parquet";
	copy_statement.info->is_from = false;
	copy_statement.info->is_format_auto_detected = false;
	copy_statement.info->options["field_ids"].push_back(current_schema.GetFieldIds());
	copy_statement.info->options["filename_pattern"].push_back(Value("{uuidv7}"));
	copy_statement.info->options["file_size_bytes"].push_back(
	    Value::UBIGINT(static_cast<uint64_t>(plan.target_file_size_bytes)));
	copy_statement.info->options["return_stats"].push_back(Value::BOOLEAN(true));
	copy_statement.info->options["per_thread_output"].push_back(Value::BOOLEAN(false));
	copy_statement.info->options["append"].push_back(Value::BOOLEAN(true));

	auto copy_binder = Binder::CreateBinder(binder.context, &binder);
	auto bound_copy = copy_binder->Bind(copy_statement);
	FilterCandidateFiles(*bound_copy.plan, plan.selected_candidates);
	return std::move(bound_copy.plan);
}

static unique_ptr<LogicalOperator> RewriteDataFilesBindOperator(ClientContext &context, TableFunctionBindInput &input,
                                                                TableIndex bind_index,
                                                                vector<Identifier> &return_names) {
	if (!input.binder) {
		throw InternalException("iceberg_rewrite_data_files: bind_operator called without a binder");
	}
	auto plan_input = ParseRewritePlanInput(context, input);
	input.binder->SetAlwaysRequireRebind();
	auto plan = PlanRewrite(context, plan_input);

	auto result = make_uniq<LogicalRewriteDataFiles>(bind_index.index, std::move(plan));
	if (!result->plan.selected_candidates.empty()) {
		result->children.push_back(BindCandidateCopy(*input.binder, result->plan));
	}

	return_names = {"rewritten_data_files_count", "added_data_files_count", "rewritten_bytes_count",
	                "failed_data_files_count", "removed_delete_files_count"};
	return std::move(result);
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergRewriteDataFilesFunction() {
	TableFunctionSet function_set("iceberg_rewrite_data_files");
	TableFunction function("iceberg_rewrite_data_files",
	                       FunctionSignature().AddPositionalOnly("table_name", LogicalType::VARCHAR), nullptr);
	function.bind_operator = RewriteDataFilesBindOperator;
	function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("target_file_size_bytes", LogicalType::ANY)
		    .Add("min_file_size_bytes", LogicalType::ANY)
		    .Add("max_file_size_bytes", LogicalType::ANY)
		    .Add("min_input_files", LogicalType::BIGINT)
		    .Add("max_files_to_rewrite", LogicalType::BIGINT)
		    .Add("rewrite_all", LogicalType::BOOLEAN)
		    .Add("output_spec_id", LogicalType::INTEGER);
	});
	function_set.AddFunction(function);
	return function_set;
}

} // namespace duckdb
