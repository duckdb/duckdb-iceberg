#include "planning/iceberg_multi_file_list.hpp"
#include "planning/iceberg_multi_file_reader.hpp"

#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "duckdb/common/column_index_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"
#include "duckdb/storage/table/row_group_reorderer.hpp"

#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

namespace duckdb {

namespace {

static bool TryGetStructExtractPath(unique_ptr<Expression> &expr_p, optional_ptr<BoundColumnRefExpression> &column_ref,
                                    vector<idx_t> &path_components) {
	auto &expr = *expr_p;
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &ref = expr.Cast<BoundColumnRefExpression>();
		if (ref.GetReturnType().id() != LogicalTypeId::STRUCT) {
			return false;
		}
		column_ref = ref;
		return true;
	}
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &func = expr.Cast<BoundFunctionExpression>();
	idx_t child_idx;
	if (!TryGetStructExtractChildIndex(func, child_idx) || func.GetChildren().empty()) {
		return false;
	}
	if (!TryGetStructExtractPath(func.GetChildrenMutable()[0], column_ref, path_components)) {
		return false;
	}
	path_components.push_back(child_idx);
	return true;
}

static ColumnIndex CreateColumnIndexPath(const vector<idx_t> &path_components) {
	D_ASSERT(!path_components.empty());
	ColumnIndex path(path_components[0]);
	auto *current = &path;
	for (idx_t i = 1; i < path_components.size(); i++) {
		current->AddChildIndex(ColumnIndex(path_components[i]));
		current = &current->GetChildIndex(0);
	}
	return path;
}

static ColumnIndex &GetColumnIndexLeaf(ColumnIndex &column_index) {
	auto *current = &column_index;
	while (current->HasChildren()) {
		D_ASSERT(current->ChildIndexCount() == 1);
		current = &current->GetChildIndex(0);
	}
	return *current;
}

static ColumnIndex CreatePushdownExtractColumnIndex(const ColumnIndex &base_index, const LogicalType &base_type,
                                                    const ColumnIndex &extract_path) {
	ColumnIndex result = base_index;
	if (result.IsPushdownExtract()) {
		auto &leaf = GetColumnIndexLeaf(result.GetChildIndex(0));
		leaf.AddChildIndex(extract_path);
		result.SetPushdownExtractType(result.GetType());
	} else {
		result.GetChildIndexesMutable().clear();
		result.AddChildIndex(extract_path);
		result.SetPushdownExtractType(base_type);
	}
	return result;
}

static bool RewriteFilterPushdownExtracts(unique_ptr<Expression> &expr, TableIndex table_index,
                                          vector<ColumnIndex> &column_indexes,
                                          column_index_map<ProjectionIndex> &projection_map) {
	optional_ptr<BoundColumnRefExpression> column_ref;
	vector<idx_t> path_components;
	if (TryGetStructExtractPath(expr, column_ref, path_components)) {
		if (path_components.empty()) {
			return false;
		}
		auto extract_path = CreateColumnIndexPath(path_components);
		auto &binding = column_ref->Binding();
		if (binding.table_index != table_index || binding.column_index.GetIndex() >= column_indexes.size()) {
			return false;
		}
		auto projected_column_index = CreatePushdownExtractColumnIndex(column_indexes[binding.column_index.GetIndex()],
		                                                               column_ref->GetReturnType(), extract_path);
		auto entry = projection_map.find(projected_column_index);
		ProjectionIndex projection_index;
		if (entry == projection_map.end()) {
			projection_index = ProjectionIndex(column_indexes.size());
			projection_map.emplace(projected_column_index, projection_index);
			column_indexes.push_back(projected_column_index);
		} else {
			projection_index = entry->second;
		}
		expr = make_uniq<BoundColumnRefExpression>(expr->GetReturnType(), ColumnBinding(table_index, projection_index),
		                                           column_ref->Depth());
		return true;
	}

	bool rewritten = false;
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		rewritten |= RewriteFilterPushdownExtracts(child, table_index, column_indexes, projection_map);
	});
	return rewritten;
}

static TableFilterSet GenerateTableScanFilters(ClientContext &context, vector<ColumnIndex> &column_indexes,
                                               const vector<unique_ptr<Expression>> &filters) {
	column_index_map<ProjectionIndex> projection_map;
	projection_map.reserve(column_indexes.size());
	for (idx_t i = 0; i < column_indexes.size(); i++) {
		projection_map.emplace(column_indexes[i], ProjectionIndex(i));
	}

	vector<unique_ptr<Expression>> rewritten_filters;
	rewritten_filters.reserve(filters.size());
	for (const auto &filter : filters) {
		rewritten_filters.push_back(filter->Copy());
	}
	// Split AND predicates into separate filters so that we can push down each filter individually
	LogicalFilter::SplitPredicates(rewritten_filters);

	FilterCombiner combiner(context);
	for (auto &filter : rewritten_filters) {
		RewriteFilterPushdownExtracts(filter, TableIndex(0) /* FIXME */, column_indexes, projection_map);
		combiner.AddFilter(std::move(filter));
	}

	vector<FilterPushdownResult> unused;
	return combiner.GenerateTableScanFilters(column_indexes, unused);
}

//! Like TryGetStructExtractPath, but for filter expressions from a TableFilterSet, whose subject is a
//! BoundReferenceExpression(0) placeholder rather than a BoundColumnRefExpression.
static bool TryGetStructExtractPathFromRef(unique_ptr<Expression> &expr_p, optional_ptr<BoundReferenceExpression> &ref,
                                           vector<idx_t> &path_components) {
	auto &expr = *expr_p;
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		ref = expr.Cast<BoundReferenceExpression>();
		return true;
	}
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &func = expr.Cast<BoundFunctionExpression>();
	idx_t child_idx;
	if (!TryGetStructExtractChildIndex(func, child_idx) || func.GetChildren().empty()) {
		return false;
	}
	if (!TryGetStructExtractPathFromRef(func.GetChildrenMutable()[0], ref, path_components)) {
		return false;
	}
	path_components.push_back(child_idx);
	return true;
}

//! Rewrites struct_extract chains rooted in a BoundReferenceExpression(0) into a pushdown-extract column index,
//! updating 'projection_index' to point at the (possibly newly created) projected column when a rewrite happens.
static bool RewriteDynamicFilterPushdownExtracts(unique_ptr<Expression> &expr, const ColumnIndex &base_column_index,
                                                 vector<ColumnIndex> &column_indexes,
                                                 column_index_map<ProjectionIndex> &projection_map,
                                                 ProjectionIndex &projection_index) {
	optional_ptr<BoundReferenceExpression> ref;
	vector<idx_t> path_components;
	if (TryGetStructExtractPathFromRef(expr, ref, path_components)) {
		if (path_components.empty()) {
			return false;
		}
		auto extract_path = CreateColumnIndexPath(path_components);
		auto projected_column_index =
		    CreatePushdownExtractColumnIndex(base_column_index, ref->GetReturnType(), extract_path);
		auto entry = projection_map.find(projected_column_index);
		if (entry == projection_map.end()) {
			projection_index = ProjectionIndex(column_indexes.size());
			projection_map.emplace(projected_column_index, projection_index);
			column_indexes.push_back(projected_column_index);
		} else {
			projection_index = entry->second;
		}
		expr = make_uniq<BoundReferenceExpression>(expr->GetReturnType(), 0);
		return true;
	}

	bool rewritten = false;
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		rewritten |= RewriteDynamicFilterPushdownExtracts(child, base_column_index, column_indexes, projection_map,
		                                                  projection_index);
	});
	return rewritten;
}

//! Generates a TableFilterSet from an existing TableFilterSet whose filter subjects are BoundReferenceExpression(0)
//! placeholders (as produced by MultiFileDynamicPushdownInfo), rewriting struct_extract chains into pushdown-extract
//! column indexes, potentially adding new entries to 'column_indexes'.
static TableFilterSet GenerateDynamicTableScanFilters(vector<ColumnIndex> &column_indexes, TableFilterSet &filters) {
	column_index_map<ProjectionIndex> projection_map;
	projection_map.reserve(column_indexes.size());
	for (idx_t i = 0; i < column_indexes.size(); i++) {
		projection_map.emplace(column_indexes[i], ProjectionIndex(i));
	}

	TableFilterSet result;
	for (auto &entry : filters) {
		auto base_projection_index = entry.GetIndex();
		//! Copy, not reference: 'column_indexes' may be reallocated by a rewrite below, across split predicates
		auto base_column_index = column_indexes[base_projection_index.GetIndex()];
		auto &filter =
		    ExpressionFilter::GetExpressionFilter(entry.Filter(), "IcebergMultiFileList::DynamicFilterPushdown");

		vector<unique_ptr<Expression>> split_expressions;
		split_expressions.push_back(filter.expr->Copy());
		// Split AND predicates into separate filters so that we can push down each filter individually
		LogicalFilter::SplitPredicates(split_expressions);

		for (auto &split_expr : split_expressions) {
			auto projection_index = base_projection_index;
			RewriteDynamicFilterPushdownExtracts(split_expr, base_column_index, column_indexes, projection_map,
			                                     projection_index);
			if (!column_indexes[projection_index.GetIndex()].HasPrimaryIndex()) {
				//! Field-identifier column indexes (e.g. variant subfields) aren't supported by the pruner/schema
				//! lookup; leave the predicate as a residual filter instead of pushing it down.
				continue;
			}
			result.PushFilter(projection_index, make_uniq<ExpressionFilter>(std::move(split_expr)));
		}
	}
	return result;
}

} // namespace

IcebergMultiFileList::IcebergMultiFileList(ClientContext &context, shared_ptr<IcebergScanInfo> scan_info,
                                           const string &path, const IcebergOptions &options)
    : planner(make_uniq<IcebergScanPlanner>(context, std::move(scan_info), path, options)),
      delete_execution(make_shared_ptr<IcebergDeleteExecutionState>()) {
}

IcebergMultiFileList::IcebergMultiFileList(unique_ptr<IcebergScanPlanner> planner_p,
                                           shared_ptr<IcebergDeleteExecutionState> delete_execution_p)
    : planner(std::move(planner_p)), delete_execution(std::move(delete_execution_p)) {
}

IcebergMultiFileList::~IcebergMultiFileList() {
}

IcebergScanPlanner &IcebergMultiFileList::GetScanPlanner() {
	return *planner;
}

const IcebergScanPlanner &IcebergMultiFileList::GetScanPlanner() const {
	return *planner;
}

IcebergDeleteExecutionState &IcebergMultiFileList::GetDeleteReader() const {
	return *delete_execution;
}

void IcebergMultiFileList::SetTable(IcebergTableSchemaVersion &table) {
	planner->SetTable(table);
}

optional_ptr<IcebergTableSchemaVersion> IcebergMultiFileList::GetTable() const {
	return planner->GetTable();
}

void IcebergMultiFileList::Bind(vector<LogicalType> &return_types, vector<Identifier> &names) {
	if (have_bound) {
		names = StringsToIdentifiers(this->names);
		return_types = types;
		return;
	}
	if (!planner->HasScanInfo()) {
		D_ASSERT(!planner->GetPath().empty());
		auto resolved_metadata =
		    IcebergUtils::ResolveTableMetadata(planner->GetContext(), planner->GetPath(), planner->GetOptions());
		auto temp_data = make_uniq<IcebergScanTemporaryData>(std::move(resolved_metadata.metadata));
		auto &metadata = temp_data->metadata;
		auto snapshot_info = metadata.GetSnapshot(*planner->GetOptions().snapshot_lookup);
		auto &schema = metadata.GetSchemaFromId(snapshot_info.schema_id);
		planner->SetScanInfo(make_shared_ptr<IcebergScanInfo>(resolved_metadata.table_location, std::move(temp_data),
		                                                      snapshot_info, schema));
	}
	for (auto &schema_entry : planner->GetSchema().columns) {
		names.push_back(Identifier(schema_entry->name));
		return_types.push_back(schema_entry->type);
	}
	QueryResult::DeduplicateColumns(names);
	for (idx_t i = 0; i < names.size(); i++) {
		planner->GetSchema().columns[i]->name = names[i].GetIdentifierName();
	}
	have_bound = true;
	this->names = IdentifiersToStrings(names);
	types = return_types;
}

shared_ptr<IcebergDeleteData> IcebergMultiFileList::GetExistingPositionalDeleteData(const string &file_path) const {
	return delete_execution->GetExistingPositionalDeleteData(file_path);
}

IcebergDeletePlan IcebergMultiFileList::ProcessDeletes(const IcebergFileScanTask &task) const {
	IcebergDeleteExecutionContext execution {planner->GetContext(), FileSystem::GetFileSystem(planner->GetContext()),
	                                         planner->GetPath(), planner->GetOptions(), planner->GetMetadata()};
	return delete_execution->ProcessDeletes(execution, task.original_file_path, task.delete_files);
}

unique_ptr<IcebergMultiFileList>
IcebergMultiFileList::PushdownInternal(TableFilterSet &new_filters, const vector<ColumnIndex> &column_indexes) const {
	IcebergTableFilters result_filter_set;
	for (auto &entry : new_filters) {
		auto projection_index = ProjectionIndex(entry.GetIndex().GetIndex());
		auto &column_index = column_indexes[projection_index];
		auto primary_index = column_index.GetPrimaryIndex();
		if (primary_index >= names.size()) {
			continue;
		}
		auto &filter = ExpressionFilter::GetExpressionFilter(entry.Filter(), "IcebergMultiFileList::PushdownInternal");
		result_filter_set.PushFilter(column_index, filter.Copy());
	}
	auto result = unique_ptr<IcebergMultiFileList>(
	    new IcebergMultiFileList(planner->CreateView(std::move(result_filter_set)), delete_execution));
	result->have_bound = true;
	result->names = names;
	result->types = types;
	return result;
}

unique_ptr<MultiFileList>
IcebergMultiFileList::DynamicFilterPushdown(MultiFileDynamicPushdownInfo &pushdown_info) const {
	auto column_indexes = pushdown_info.column_indexes;
	auto &filters = pushdown_info.filters;
	if (!filters.HasFilters()) {
		return nullptr;
	}

	// Convert any struct_extract expressions in the filters to references with pushdown-extract column indexes
	auto rewritten_filters = GenerateDynamicTableScanFilters(column_indexes, filters);

	bool filters_changed = false;
	for (auto &entry : rewritten_filters) {
		auto &filter =
		    ExpressionFilter::GetExpressionFilter(entry.Filter(), "IcebergMultiFileList::DynamicFilterPushdown");
		auto column_id = column_indexes[entry.GetIndex().GetIndex()];
		auto previously_pushed_down_filter = planner->Filters().TryGetFilterByColumnIndex(column_id);
		if (!previously_pushed_down_filter || !filter.Equals(*previously_pushed_down_filter)) {
			filters_changed = true;
		}
	}

	if (filters_changed) {
		// Dynamic filter pushdown supplies the complete effective filter for every column. This includes filters
		// already pushed down by ComplexFilterPushdown, potentially combined with a new runtime filter.
		auto new_snap = PushdownInternal(rewritten_filters, column_indexes);
		return std::move(new_snap);
	}
	return nullptr;
}

unique_ptr<MultiFileList> IcebergMultiFileList::ComplexFilterPushdown(ClientContext &context, const MultiFileOptions &,
                                                                      MultiFilePushdownInfo &info,
                                                                      vector<unique_ptr<Expression>> &filters) const {
	if (filters.empty()) {
		return nullptr;
	}

	// Convert any struct_extract expressions in the filters to column references with pushdown-extract column indexes
	vector<unique_ptr<Expression>> rewritten_filters;
	rewritten_filters.reserve(filters.size());
	for (const auto &filter : filters) {
		rewritten_filters.push_back(filter->Copy());
	}

	auto column_indexes = info.column_indexes;
	auto filter_set = GenerateTableScanFilters(context, column_indexes, filters);
	if (!filter_set.HasFilters()) {
		return nullptr;
	}

	return PushdownInternal(filter_set, column_indexes);
}

OpenFileInfo IcebergMultiFileList::GetFileInternal(idx_t file_id) const {
	auto task = planner->GetDataFileDescriptor(file_id);
	if (!task) {
		return OpenFileInfo();
	}
	return IcebergMultiFileReader::FileInfo(task->file_path, task->file_format, task->file_size_in_bytes,
	                                        task->first_row_id, task->sequence_number);
}

vector<OpenFileInfo> IcebergMultiFileList::GetAllFiles() const {
	vector<OpenFileInfo> result;
	for (idx_t i = 0;; i++) {
		auto file = GetFileInternal(i);
		if (file.path.empty()) {
			break;
		}
		result.push_back(std::move(file));
	}
	return result;
}

FileExpandResult IcebergMultiFileList::GetExpandResult() const {
	if (have_bound) {
		GetFileInternal(1);
	}
	return FileExpandResult::MULTIPLE_FILES;
}

idx_t IcebergMultiFileList::GetTotalFileCount() const {
	return planner->GetTotalFileCount();
}

unique_ptr<NodeStatistics> IcebergMultiFileList::GetCardinality(ClientContext &) const {
	return planner->GetCardinality();
}

OpenFileInfo IcebergMultiFileList::GetFile(idx_t file_id) const {
	return GetFileInternal(file_id);
}

} // namespace duckdb
