#include "planning/iceberg_multi_file_list.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/task_notifier.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"
#include "duckdb/storage/table/row_group_reorderer.hpp"

#include "planning/iceberg_multi_file_reader.hpp"
#include "function/iceberg_functions.hpp"
#include "planning/metadata_io/deletes/iceberg_deletes_file_reader.hpp"
#include "common/iceberg_utils.hpp"
#include "iceberg_logging.hpp"
#include "planning/pruning/iceberg_predicate.hpp"
#include "core/expression/iceberg_value.hpp"
#include "duckdb/storage/statistics/geometry_stats.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "core/expression/iceberg_predicate_stats.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "storage/statistics/iceberg_variant_statistics.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/metadata_io/manifest_list/iceberg_manifest_list_reader.hpp"
#include "planning/metadata_io/manifest_list/bound_iceberg_manifest_list_entry.hpp"

#include <algorithm>

namespace duckdb {

void ManifestEntryReadState::PushBatch(ManifestReadBatch &&batch) {
	lock_guard<mutex> guard(lock);
	batches.push_back(std::move(batch));
}

bool ManifestEntryReadState::GetBatch(idx_t batch_idx, ManifestReadBatch &result) const {
	lock_guard<mutex> guard(lock);
	if (batch_idx >= batches.size()) {
		return false;
	}
	result = batches[batch_idx];
	return true;
}

namespace {

static unique_ptr<Expression> CreateReferenceExpression(const LogicalType &type) {
	return make_uniq<BoundReferenceExpression>(type, 0ULL);
}

static void AppendColumnPath(const ColumnIndex &column_index, vector<idx_t> &path) {
	for (auto &child_index : column_index.GetChildIndexes()) {
		path.push_back(child_index.GetPrimaryIndex());
		AppendColumnPath(child_index, path);
	}
}

static vector<idx_t> GetColumnPath(const ColumnIndex &column_index) {
	column_index.VerifySinglePath();
	vector<idx_t> path;
	AppendColumnPath(column_index, path);
	return path;
}

static bool TryGetFilterPath(const Expression &expr, vector<idx_t> &path) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_COLUMN_REF:
		return true;
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		idx_t child_idx;
		if (!TryGetStructExtractChildIndex(func, child_idx) || func.GetChildren().empty()) {
			return false;
		}
		if (!TryGetFilterPath(*func.GetChildren()[0], path)) {
			return false;
		}
		path.push_back(child_idx);
		return true;
	}
	default:
		return false;
	}
}

enum class FilterPathMatch : uint8_t { NONE, MATCH, OTHER };

static FilterPathMatch GetFilterPathMatch(const Expression &expr, const vector<idx_t> &path) {
	vector<idx_t> expr_path;
	if (TryGetFilterPath(expr, expr_path)) {
		return expr_path == path ? FilterPathMatch::MATCH : FilterPathMatch::OTHER;
	}
	auto result = FilterPathMatch::NONE;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (result == FilterPathMatch::OTHER) {
			return;
		}
		auto child_result = GetFilterPathMatch(child, path);
		if (child_result == FilterPathMatch::OTHER) {
			result = FilterPathMatch::OTHER;
		} else if (child_result == FilterPathMatch::MATCH) {
			result = FilterPathMatch::MATCH;
		}
	});
	return result;
}

static bool MatchesFilterPath(const Expression &expr, const vector<idx_t> &path) {
	vector<idx_t> expr_path;
	return TryGetFilterPath(expr, expr_path) && expr_path == path;
}

static void ReplaceFilterPathExpressions(unique_ptr<Expression> &expr, const vector<idx_t> &path) {
	if (MatchesFilterPath(*expr, path)) {
		expr = CreateReferenceExpression(expr->GetReturnType());
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { ReplaceFilterPathExpressions(child, path); });
}

static unique_ptr<Expression> ExtractFilterExpressionForPath(const Expression &expr, const vector<idx_t> &path) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.Function().GetName() == OptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<OptionalFilterFunctionData>();
			return data.child_filter_expr ? ExtractFilterExpressionForPath(*data.child_filter_expr, path) : nullptr;
		}
		if (func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			return data.child_filter_expr ? ExtractFilterExpressionForPath(*data.child_filter_expr, path) : nullptr;
		}
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		auto result = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &child : conjunction.GetChildren()) {
			auto extracted_child = ExtractFilterExpressionForPath(*child, path);
			if (extracted_child) {
				result->GetChildrenMutable().push_back(std::move(extracted_child));
			}
		}
		if (result->GetChildren().empty()) {
			return nullptr;
		}
		if (result->GetChildren().size() == 1) {
			return std::move(result->GetChildrenMutable()[0]);
		}
		return std::move(result);
	}
	if (GetFilterPathMatch(expr, path) != FilterPathMatch::MATCH) {
		return nullptr;
	}
	auto result = expr.Copy();
	ReplaceFilterPathExpressions(result, path);
	return result;
}

class ManifestReadTask : public BaseExecutorTask {
public:
	ManifestReadTask(IcebergManifestScanningState &state)
	    : BaseExecutorTask(state.executor), state(state), reader(*state.scan) {
	}

	void ExecuteTask() override {
		throw InternalException("Simple ExecuteTask should never be called!");
	}

	TaskExecutionResult ExecuteTaskIncremental() {
		while (!reader.Finished()) {
			reader.Read();
			return TaskExecutionResult::TASK_NOT_FINISHED;
		}
		--state.in_progress_tasks;
		return TaskExecutionResult::TASK_FINISHED;
	}

	TaskExecutionResult Execute(TaskExecutionMode mode) override {
		if (executor.HasError()) {
			// another task encountered an error - bailout
			executor.FinishTask();
			return TaskExecutionResult::TASK_FINISHED;
		}
		try {
			{
				TaskNotifier task_notifier {state.context};
				auto res = TaskExecutionResult::TASK_NOT_FINISHED;
				while (res == TaskExecutionResult::TASK_NOT_FINISHED) {
					res = ExecuteTaskIncremental();
					if (res == TaskExecutionResult::TASK_NOT_FINISHED && mode == TaskExecutionMode::PROCESS_PARTIAL) {
						return res;
					}
				}
			}
			executor.FinishTask();
			return TaskExecutionResult::TASK_FINISHED;
		} catch (std::exception &ex) {
			executor.PushError(ErrorData(ex));
		} catch (...) { // LCOV_EXCL_START
			executor.PushError(ErrorData("Unknown exception during Checkpoint!"));
		} // LCOV_EXCL_STOP
		executor.FinishTask();
		return TaskExecutionResult::TASK_ERROR;
	}

private:
	IcebergManifestScanningState &state;
	manifest_file::ManifestReader reader;
};

} // namespace

IcebergMultiFileList::IcebergMultiFileList(ClientContext &context_p, shared_ptr<IcebergScanInfo> scan_info,
                                           const string &path, const IcebergOptions &options)
    : shared_state(make_shared_ptr<IcebergMultiFileListSharedState>(context_p, std::move(scan_info), path, options)),
      context(shared_state->context), fs(shared_state->fs), options(shared_state->options) {
}

IcebergMultiFileList::IcebergMultiFileList(shared_ptr<IcebergMultiFileListSharedState> shared_state_p)
    : shared_state(std::move(shared_state_p)), context(shared_state->context), fs(shared_state->fs),
      options(shared_state->options) {
}

IcebergMultiFileList::~IcebergMultiFileList() {
}

IcebergMultiFileListSharedState::IcebergMultiFileListSharedState(ClientContext &context_p,
                                                                 shared_ptr<IcebergScanInfo> scan_info_p, string path_p,
                                                                 const IcebergOptions &options_p)
    : context(context_p), fs(FileSystem::GetFileSystem(context)), scan_info(std::move(scan_info_p)),
      path(std::move(path_p)), options(options_p) {
}

IcebergMultiFileListSharedState::~IcebergMultiFileListSharedState() {
	if (data_manifest_read_state) {
		//! FIXME: this could throw, if the tasks encountered an error
		data_manifest_read_state->executor.WorkOnTasks();
	}
}

string IcebergMultiFileList::ToDuckDBPath(const string &raw_path) {
	return raw_path;
}

string IcebergMultiFileList::GetPath() const {
	return shared_state->path;
}

const IcebergTableMetadata &IcebergMultiFileList::GetMetadata() const {
	return shared_state->scan_info->metadata;
}

bool IcebergMultiFileList::HasTransactionData() const {
	return shared_state->scan_info->transaction_data;
}

const IcebergTransactionData &IcebergMultiFileList::GetTransactionData() const {
	D_ASSERT(HasTransactionData());
	return *shared_state->scan_info->transaction_data;
}

const IcebergSnapshotScanInfo &IcebergMultiFileList::GetSnapshot() const {
	return shared_state->scan_info->snapshot_info;
}

const IcebergTableSchema &IcebergMultiFileList::GetSchema() const {
	return shared_state->scan_info->schema;
}

bool IcebergMultiFileList::FinishedScanningDeletes() const {
	return !shared_state->delete_manifest_reader || shared_state->delete_manifest_reader->Finished();
}

IcebergTableEntry *IcebergMultiFileList::GetTable() const {
	return shared_state->table;
}

void IcebergMultiFileList::SetTable(IcebergTableEntry *table) {
	shared_state->table = table;
}

void IcebergMultiFileList::SetOptions(const IcebergOptions &options) {
	shared_state->options = options;
}

void IcebergMultiFileList::SetScanOrder(unique_ptr<RowGroupOrderOptions> options) {
	scan_order_options = std::move(options);
	scan_order_applied = false;
}

unique_ptr<ExpressionFilter> IcebergMultiFileList::GetFilterForColumnIndex(const IcebergTableFilters &filter_set,
                                                                           const ColumnIndex &column_index) const {
	auto primary_index = column_index.GetPrimaryIndex();

	auto filter = filter_set.TryGetFilterByColumnIndex(primary_index);
	if (!filter) {
		return nullptr;
	}

	auto path = GetColumnPath(column_index);
	if (path.empty()) {
		return filter->Copy();
	}

	auto child_expr = ExtractFilterExpressionForPath(*filter->expr, path);
	if (!child_expr) {
		//! This filter is not targeting the column on which a partition exists
		return nullptr;
	}
	return make_uniq<ExpressionFilter>(std::move(child_expr));
}

void IcebergMultiFileList::Bind(vector<LogicalType> &return_types, vector<Identifier> &names) {
	lock_guard<mutex> guard(shared_state->lock);

	if (have_bound) {
		names = StringsToIdentifiers(this->names);
		return_types = this->types;
		return;
	}
	if (!shared_state->scan_info) {
		D_ASSERT(!shared_state->path.empty());
		auto input_string = shared_state->path;
		auto resolved_metadata = IcebergUtils::ResolveTableMetadata(context, input_string, options);

		auto temp_data = make_uniq<IcebergScanTemporaryData>();
		temp_data->metadata = std::move(resolved_metadata.metadata);
		auto &metadata = temp_data->metadata;

		IcebergSnapshotScanInfo snapshot_info;
		snapshot_info = metadata.GetSnapshot(*options.snapshot_lookup);
		auto schema = metadata.GetSchemaFromId(snapshot_info.schema_id);
		shared_state->scan_info = make_shared_ptr<IcebergScanInfo>(resolved_metadata.table_location,
		                                                           std::move(temp_data), snapshot_info, *schema);
	}

	auto &schema = GetSchema().columns;
	for (auto &schema_entry : schema) {
		names.push_back(Identifier(schema_entry->name));
		return_types.push_back(schema_entry->type);
	}

	QueryResult::DeduplicateColumns(names);
	for (idx_t i = 0; i < names.size(); i++) {
		schema[i]->name = names[i].GetIdentifierName();
	}

	have_bound = true;
	this->names = IdentifiersToStrings(names);
	this->types = return_types;
}

unique_ptr<IcebergMultiFileList> IcebergMultiFileList::PushdownInternal(ClientContext &context,
                                                                        TableFilterSet &new_filters,
                                                                        const vector<column_t> &column_indexes) const {
	auto filtered_list = unique_ptr<IcebergMultiFileList>(new IcebergMultiFileList(shared_state));

	IcebergTableFilters result_filter_set;

	// The supplied filter set is the complete set of filters for the new view.
	for (auto &entry : new_filters) {
		auto column_idx = column_indexes[entry.GetIndex().GetIndex()];
		if (column_idx < names.size()) {
			auto &filter =
			    ExpressionFilter::GetExpressionFilter(entry.Filter(), "IcebergMultiFileList::PushdownInternal");
			result_filter_set.PushFilter(column_idx, filter.Copy());
		}
	}

	filtered_list->table_filters = std::move(result_filter_set);
	filtered_list->names = names;
	filtered_list->types = types;
	filtered_list->have_bound = true;
	if (scan_order_options) {
		filtered_list->scan_order_options = make_uniq<RowGroupOrderOptions>(*scan_order_options);
	}
	return filtered_list;
}

unique_ptr<MultiFileList>
IcebergMultiFileList::DynamicFilterPushdown(ClientContext &context, const MultiFileOptions &options,
                                            const vector<Identifier> &names, const vector<LogicalType> &types,
                                            const vector<column_t> &column_ids, TableFilterSet &filters) const {
	if (!filters.HasFilters()) {
		return nullptr;
	}

	auto filters_copy = filters.Copy();
	D_ASSERT(filters_copy->FilterCount() >= table_filters.FilterCount());
	bool filters_changed = false;
	for (auto &entry : filters) {
		auto &filter =
		    ExpressionFilter::GetExpressionFilter(entry.Filter(), "IcebergMultiFileList::DynamicFilterPushdown");
		auto column_id = column_ids[entry.GetIndex().GetIndex()];
		auto previously_pushed_down_filter = table_filters.TryGetFilterByColumnIndex(column_id);
		if (!previously_pushed_down_filter || !filter.Equals(*previously_pushed_down_filter)) {
			filters_changed = true;
		}
	}

	if (filters_changed) {
		// Dynamic filter pushdown supplies the complete effective filter for every column. This includes filters
		// already pushed down by ComplexFilterPushdown, potentially combined with a new runtime filter.
		auto new_snap = PushdownInternal(context, *filters_copy, column_ids);
		return std::move(new_snap);
	}
	return nullptr;
}

unique_ptr<MultiFileList> IcebergMultiFileList::ComplexFilterPushdown(ClientContext &context,
                                                                      const MultiFileOptions &options,
                                                                      MultiFilePushdownInfo &info,
                                                                      vector<unique_ptr<Expression>> &filters) const {
	if (filters.empty()) {
		return nullptr;
	}

	FilterCombiner combiner(context);
	for (const auto &filter : filters) {
		combiner.AddFilter(filter->Copy());
	}

	vector<FilterPushdownResult> unused;
	auto filter_set = combiner.GenerateTableScanFilters(info.column_indexes, unused);
	if (!filter_set.HasFilters()) {
		return nullptr;
	}

	return PushdownInternal(context, filter_set, info.column_ids);
}

vector<OpenFileInfo> IcebergMultiFileList::GetAllFiles() const {
	vector<OpenFileInfo> file_list;
	//! Lock is required because it reads the 'manifest_entries' vector
	lock_guard<mutex> guard(shared_state->lock);
	for (idx_t i = 0; i < data_manifest_entries.size(); i++) {
		file_list.push_back(GetFileInternal(i, guard));
	}
	return file_list;
}

FileExpandResult IcebergMultiFileList::GetExpandResult() const {
	// GetFileInternal(1) will ensure files with index 0 and index 1 are expanded if they are available
	lock_guard<mutex> guard(shared_state->lock);
	GetFileInternal(1, guard);

	// always return multiple files, In the case there is only 1 data file,
	// we only lose performance if it is small
	return FileExpandResult::MULTIPLE_FILES;
}

idx_t IcebergMultiFileList::GetTotalFileCount() const {
	// FIXME: the 'added_files_count' + the 'existing_files_count'
	// in the Manifest List should give us this information without scanning the manifest file(s)
	lock_guard<mutex> guard(shared_state->lock);

	idx_t i = data_manifest_entries.size();
	while (!GetFileInternal(i, guard).path.empty()) {
		i++;
	}
	return data_manifest_entries.size();
}

unique_ptr<NodeStatistics> IcebergMultiFileList::GetCardinality(ClientContext &context) const {
	if (GetMetadata().iceberg_version == 1) {
		//! We collect no cardinality information from manifests for V1 tables.
		return nullptr;
	}

	lock_guard<mutex> guard(shared_state->lock);
	InitializeView(guard);

	idx_t cardinality = 0;
	for (idx_t i = 0; i < data_manifests.size(); i++) {
		auto &manifest = data_manifests[i].entry.file;
		if (!data_manifest_matches[i]) {
			continue;
		}
		cardinality += manifest.added_rows_count;
		cardinality += manifest.existing_rows_count;
	}
	for (idx_t i = 0; i < delete_manifests.size(); i++) {
		auto &manifest = delete_manifests[i].entry.file;
		if (!delete_manifest_matches[i]) {
			continue;
		}
		cardinality -= manifest.added_rows_count;
	}
	return make_uniq<NodeStatistics>(cardinality, cardinality);
}

BoundIcebergManifestEntry IcebergMultiFileList::GetManifestEntry(idx_t file_id) const {
	lock_guard<mutex> guard(shared_state->lock);
	return data_manifest_entries[file_id];
}

vector<IcebergPartitionInfo> IcebergMultiFileList::GetPartitionInfoForDataFile(const string &file_path) const {
	lock_guard<mutex> guard(shared_state->lock);
	auto entry = shared_state->data_file_partition_info.find(file_path);
	if (entry != shared_state->data_file_partition_info.end()) {
		return entry->second;
	}
	throw InternalException("Could not find data file '%s' in manifest entries", file_path);
}

const IcebergManifestFile &IcebergMultiFileList::GetManifestFileForEntry(const BoundIcebergManifestEntry &entry,
                                                                         IcebergManifestContentType type) const {
	if (type == IcebergManifestContentType::DATA) {
		return data_manifests[entry.manifest_file_idx].entry.file;
	} else {
		return delete_manifests[entry.manifest_file_idx].entry.file;
	}
}

void IcebergMultiFileList::GetStatistics(vector<PartitionStatistics> &result) const {
	if (GetMetadata().iceberg_version == 1) {
		//! We collect no statistics information from manifests for V1 tables.
		return;
	}
	lock_guard<mutex> guard(shared_state->lock);
	InitializeView(guard);

	for (idx_t i = 0; i < delete_manifests.size(); i++) {
		if (delete_manifest_matches[i]) {
			//! if a matching delete manifest exists, return;
			return;
		}
	}

	idx_t count = 0;
	for (idx_t i = 0; i < data_manifests.size(); i++) {
		auto &manifest = data_manifests[i].entry.file;
		if (!data_manifest_matches[i]) {
			continue;
		}
		count += manifest.existing_rows_count;
		count += manifest.added_rows_count;
	}

	PartitionStatistics partition_stats;
	partition_stats.count = count;
	partition_stats.count_type = CountType::COUNT_EXACT;
	result.push_back(partition_stats);
}

void IcebergPredicateStats::SetLowerBound(const Value &new_lower_bound) {
	lower_bound = new_lower_bound;
}

void IcebergPredicateStats::SetUpperBound(const Value &new_upper_bound) {
	upper_bound = new_upper_bound;
}

bool IcebergPredicateStats::BoundsAreNull() const {
	return lower_bound && upper_bound && lower_bound->IsNull() && upper_bound->IsNull();
}

// Iceberg v3 (Appendix D): geometry lower/upper bounds are a packed little-endian
// sequence of f64 doubles giving the min/max corner of the bounding box, in order
// x, y, (z), (m). Reconstruct a GEOMETRY_STATS BaseStatistics whose extent spans
// [lower, upper] so spatial predicate pruning can be delegated to
// GeometryStats::CheckZonemap. Returns null when the bounds can't form an XY box.
static shared_ptr<BaseStatistics> BuildGeometryStats(const Value &lower_bound, const Value &upper_bound,
                                                     const LogicalType &type) {
	if (lower_bound.IsNull() || upper_bound.IsNull()) {
		return nullptr;
	}
	auto lower_blob = lower_bound.GetValueUnsafe<string_t>();
	auto upper_blob = upper_bound.GetValueUnsafe<string_t>();
	const auto lower_coordinate_card = lower_blob.GetSize() / sizeof(double);
	const auto upper_coordinate_card = upper_blob.GetSize() / sizeof(double);
	if (lower_coordinate_card < 2 || upper_coordinate_card < 2) {
		// Not enough information to form an XY bounding box.
		return nullptr;
	}
	const auto *lo = reinterpret_cast<const double *>(lower_blob.GetData());
	const auto *hi = reinterpret_cast<const double *>(upper_blob.GetData());

	// CreateUnknown initializes the extent to ±infinity on every axis (so absent
	// Z/M axes correctly report HasZ()/HasM() == false) and sets has_no_null = true
	// so CheckZonemap doesn't short-circuit to FILTER_ALWAYS_FALSE.
	auto stats = make_shared_ptr<BaseStatistics>(GeometryStats::CreateUnknown(type));
	auto &extent = GeometryStats::GetExtent(*stats);
	extent.x_min = lo[0];
	extent.y_min = lo[1];
	extent.x_max = hi[0];
	extent.y_max = hi[1];
	// 3 doubles is XYZ (the writer emits XYZ whenever Z is present); 4 is XYZM.
	if (lower_coordinate_card >= 3 && upper_coordinate_card >= 3) {
		extent.z_min = lo[2];
		extent.z_max = hi[2];
	}
	if (lower_coordinate_card >= 4 && upper_coordinate_card >= 4) {
		extent.m_min = lo[3];
		extent.m_max = hi[3];
	}
	return stats;
}

IcebergPredicateStats IcebergPredicateStats::DeserializeBounds(const Value &lower_bound, const Value &upper_bound,
                                                               const string &name, const LogicalType &type) {
	IcebergPredicateStats res;

	if (type.id() == LogicalTypeId::GEOMETRY) {
		// Geometry bounds need both corners together to build a bounding-box extent,
		// so they don't go through the per-bound Value deserialization below.
		res.geometry_stats = BuildGeometryStats(lower_bound, upper_bound, type);
		if (res.geometry_stats == nullptr) {
			res.lower_bound.reset();
			res.upper_bound.reset();
		}
		return res;
	}

	if (!lower_bound.IsNull()) {
		D_ASSERT(lower_bound.type().id() == LogicalTypeId::BLOB);
		auto lower_bound_blob = lower_bound.GetValueUnsafe<string_t>();
		auto deserialized_lower_bound = IcebergValue::DeserializeValue(lower_bound_blob, type);
		if (deserialized_lower_bound.HasError()) {
			throw InvalidConfigurationException("Column %s lower bound deserialization failed: %s", name,
			                                    deserialized_lower_bound.GetError());
		}
		res.SetLowerBound(deserialized_lower_bound.GetValue());
	}

	if (!upper_bound.IsNull()) {
		D_ASSERT(upper_bound.type().id() == LogicalTypeId::BLOB);
		auto upper_bound_blob = upper_bound.GetValueUnsafe<string_t>();
		auto deserialized_upper_bound = IcebergValue::DeserializeValue(upper_bound_blob, type);
		if (deserialized_upper_bound.HasError()) {
			throw InvalidConfigurationException("Column %s upper bound deserialization failed: %s", name,
			                                    deserialized_upper_bound.GetError());
		}
		res.SetUpperBound(deserialized_upper_bound.GetValue());
	}
	return res;
}

bool IcebergMultiFileList::FilePartitionMatchesFilter(const IcebergDataFile &data_file,
                                                      const IcebergManifestFile &manifest_file,
                                                      const IcebergTableMetadata &metadata,
                                                      const IcebergTableSchema &schema) const {
	if (data_file.partition_info.empty()) {
		//! No bounds to check
		return true;
	}

	// check if the index is in the partition info.
	auto partition_spec_it = metadata.partition_specs.find(manifest_file.partition_spec_id);
	if (partition_spec_it == metadata.partition_specs.end()) {
		throw InvalidConfigurationException(
		    "Data file %s has partition spec %d while the metadata does not have this partition spec",
		    data_file.file_path, manifest_file.partition_spec_id);
	}
	auto &partition_spec = partition_spec_it->second;
	unordered_map<uint64_t, ColumnIndex> source_to_column_id;
	IcebergTableSchema::PopulateSourceIdMap(source_to_column_id, schema.columns, nullptr);

	//! Map from partition_field_id -> data_file.partition_info[i]
	unordered_map<uint64_t, idx_t> partition_info_map;
	for (idx_t i = 0; i < data_file.partition_info.size(); i++) {
		const auto partition_field_id = data_file.partition_info[i].field_id;
		partition_info_map.emplace(partition_field_id, i);
	}

	auto &field_summaries = partition_spec.fields;
	for (idx_t i = 0; i < field_summaries.size(); i++) {
		auto &field = partition_spec.fields[i];

		const auto &column_id = source_to_column_id.at(field.source_id);
		// Find if we have a filter for this source column
		auto table_filter = GetFilterForColumnIndex(table_filters, column_id);
		if (!table_filter) {
			continue;
		}

		// initialize dummy stats
		IcebergPredicateStats stats;
		auto it = partition_info_map.find(field.partition_field_id);
		if (it == partition_info_map.end()) {
			//! FIXME: this is an error, no??
			// continue to next partition spec field summary
			continue;
		}
		auto &partition_val = data_file.partition_info[it->second];
		stats.lower_bound = partition_val.value;
		stats.upper_bound = partition_val.value;
		// set null stats for partitioned column.
		if (partition_val.value.IsNull()) {
			// partition values can be null
			stats.has_null = true;
		} else {
			stats.has_not_null = true;
		}

		auto nan_counts_it = data_file.nan_value_counts.find(column_id.GetPrimaryIndex());
		if (nan_counts_it != data_file.nan_value_counts.end()) {
			auto &nan_counts = nan_counts_it->second;
			stats.has_nan = nan_counts != 0;
		}

		// if the filter doesn't match the partition value, we don't need to scan the data file
		if (!IcebergPredicate::MatchBounds(context, *table_filter, stats, field.transform)) {
			auto &source_column = IcebergTableSchema::GetFromColumnIndex(schema.columns, column_id, 0);
			auto partition_value_raw_str = stats.lower_bound ? stats.lower_bound->ToString() : "NULL";
			auto partition_value_transformed_str =
			    stats.lower_bound ? field.transform.PartitionValueToString(*stats.lower_bound) : "NULL";
			DUCKDB_LOG(context, IcebergLogType,
			           "Iceberg Filter Pushdown, skipped 'data_file': '%s', partition column '%s' has raw value %s "
			           "with transform '%s'. '%s(%s)=%s' does not match filter: %s",
			           data_file.file_path, source_column.name, partition_value_raw_str, field.transform.RawType(),
			           field.transform.RawType(), partition_value_raw_str, partition_value_transformed_str,
			           table_filter->ToString(source_column.name));
			return false;
		}
	}
	return true;
}

bool IcebergMultiFileList::FileMatchesFilter(const IcebergManifestFile &manifest_file,
                                             const IcebergManifestEntry &manifest_entry,
                                             IcebergManifestContentType file_type) const {
	D_ASSERT(table_filters.HasFilters());
	auto &schema = GetSchema();

	auto &metadata = GetMetadata();
	unordered_set<int32_t> mapping_field_ids;
	for (auto &mapping : metadata.mappings) {
		if (mapping.field_id != NumericLimits<int32_t>::Maximum()) {
			mapping_field_ids.insert(mapping.field_id);
		}
	}

	for (auto &entry : table_filters) {
		auto index = entry.first;
		auto &column = *schema.columns[index];

		auto &data_file = manifest_entry.data_file;
		// First check if there are partitions
		if (!FilePartitionMatchesFilter(data_file, manifest_file, metadata, schema)) {
			return false;
		}
		if (data_file.lower_bounds.empty() || data_file.upper_bounds.empty() ||
		    file_type == IcebergManifestContentType::DELETE) {
			// There are no bounds statistics for the file, can't filter,
			// or it is a delete file, which should only be filtered on partitions
			continue;
		}

		auto &column_id = column.id;
		if (!metadata.mappings.empty() && mapping_field_ids.find(column_id) == mapping_field_ids.end()) {
			// The name-mapping isn't empty, but it doesn't contain this field.
			// We take the conservative approach and assume that the name mapping is required to resolve this field.
			// i.e: assume all of these are true:
			// 1. parquet file doesn't contain field ids for this column.
			// 2. no identity transform exists for this field.
			// 3. the column has no initial-default
			// When we assume that, the column becomes unreachable (entirely NULL), voiding the stats in the iceberg
			// metadata. So we have to ignore this filter.
			continue;
		}

		auto lower_bound_it = data_file.lower_bounds.find(column_id);
		auto upper_bound_it = data_file.upper_bounds.find(column_id);
		Value lower_bound;
		Value upper_bound;
		if (lower_bound_it != data_file.lower_bounds.end()) {
			lower_bound = lower_bound_it->second;
		}
		if (upper_bound_it != data_file.upper_bounds.end()) {
			upper_bound = upper_bound_it->second;
		}
		IcebergPredicateStats stats;

		if (column.type.id() == LogicalTypeId::VARIANT) {
			if (lower_bound.IsNull() || upper_bound.IsNull()) {
				// if there are no variant stats, scan the whole file
				return true;
			}
			Value lower_decoded, upper_decoded;
			auto lower_blob = lower_bound.GetValueUnsafe<string_t>();
			auto upper_blob = upper_bound.GetValueUnsafe<string_t>();

			Value lower_variant, upper_variant;
			if (IcebergVariantBoundsReader::Deserialize(context, lower_blob, lower_decoded) &&
			    IcebergVariantBoundsReader::RekeyBoundsVariant(lower_decoded, lower_variant)) {
				stats.SetLowerBound(lower_variant);
			}
			if (IcebergVariantBoundsReader::Deserialize(context, upper_blob, upper_decoded) &&
			    IcebergVariantBoundsReader::RekeyBoundsVariant(upper_decoded, upper_variant)) {
				stats.SetUpperBound(upper_variant);
			}
		} else {
			stats = IcebergPredicateStats::DeserializeBounds(lower_bound, upper_bound, column.name, column.type);
		}

		optional<int64_t> value_count;
		optional<int64_t> null_count;
		auto value_counts_it = data_file.value_counts.find(column_id);
		if (value_counts_it != data_file.value_counts.end()) {
			value_count = value_counts_it->second;
		}

		auto null_counts_it = data_file.null_value_counts.find(column_id);
		if (null_counts_it != data_file.null_value_counts.end()) {
			null_count = null_counts_it->second;
		}

		if (null_count) {
			stats.has_null = *null_count > 0;
			if (value_count) {
				//! If both are present, we can have an accurate picture of the non-null-value count
				auto non_null_values = *value_count - *null_count;
				stats.has_not_null = non_null_values > 0;
			} else {
				// If no 'value_counts', assume there are null values
				stats.has_not_null = true;
			}
		} else {
			//! No 'null_counts' are present, conservatively assume there are nulls
			stats.has_null = true;
			if (value_count) {
				//! If there are 'value_counts' and its over 0, assume these contain non-null values
				stats.has_not_null = *value_count > 0;
			} else {
				//! If no 'value_counts', assume there are non-null values
				stats.has_not_null = true;
			}
		}

		auto nan_counts_it = data_file.nan_value_counts.find(column_id);
		if (nan_counts_it != data_file.nan_value_counts.end()) {
			auto &nan_counts = nan_counts_it->second;
			stats.has_nan = nan_counts > 0;
		} else {
			//! Assume there are nan values
			stats.has_nan = true;
		}

		auto &filter = *entry.second;
		if (!IcebergPredicate::MatchBounds(context, filter, stats, IcebergTransform::Identity())) {
			//! If any predicate fails, exclude the file
			DUCKDB_LOG(context, IcebergLogType,
			           "Iceberg Filter Pushdown, skipped 'data_file': '%s', column '%s' with "
			           "bounds [%s, %s] did not match filter: %s",
			           data_file.file_path, column.name, stats.lower_bound ? stats.lower_bound->ToString() : "N/A",
			           stats.upper_bound ? stats.upper_bound->ToString() : "N/A", filter.ToString(column.name));
			return false;
		}
	}
	return true;
}

bool IcebergMultiFileList::TryGetNextBatch(lock_guard<mutex> &guard) const {
	auto &view_cursor = data_view_cursor;
	if (view_cursor.has_current_batch) {
		return true;
	}
	if (shared_state->read_state.GetBatch(view_cursor.next_batch_idx, view_cursor.current_batch)) {
		view_cursor.next_batch_idx++;
		view_cursor.current_batch_offset = view_cursor.current_batch.start_index;
		view_cursor.has_current_batch = true;
		return true;
	}
	if (!shared_state->data_manifest_read_state) {
		return false;
	}
	auto &scheduler = TaskScheduler::GetScheduler(context);
	auto &scan_state = *shared_state->data_manifest_read_state;
	auto &executor = scan_state.executor;
	shared_ptr<Task> task_to_execute;
	while (scan_state.in_progress_tasks) {
		if (executor.GetTask(task_to_execute)) {
			auto res = task_to_execute->Execute(TaskExecutionMode::PROCESS_PARTIAL);
			if (res == TaskExecutionResult::TASK_NOT_FINISHED) {
				auto &token = *task_to_execute->token;
				scheduler.ScheduleTask(token, std::move(task_to_execute));
			}
			if (shared_state->read_state.GetBatch(view_cursor.next_batch_idx, view_cursor.current_batch)) {
				view_cursor.next_batch_idx++;
				view_cursor.current_batch_offset = view_cursor.current_batch.start_index;
				view_cursor.has_current_batch = true;
				return true;
			}
			//! We didn't manage to populate the buffer with our scan
			//! But another task might be in the process of scanning
			//! Have to wait for everything to finish to conclusively say we're done
		}
		executor.WorkOnTasks();
		break;
	}
	if (!shared_state->read_state.GetBatch(view_cursor.next_batch_idx, view_cursor.current_batch)) {
		return false;
	}
	view_cursor.next_batch_idx++;
	view_cursor.current_batch_offset = view_cursor.current_batch.start_index;
	view_cursor.has_current_batch = true;
	return true;
}

void IcebergMultiFileList::FinishScanTasks(lock_guard<mutex> &guard) const {
	if (!shared_state->data_manifest_read_state) {
		return;
	}
	auto &read_state = *shared_state->data_manifest_read_state;
	auto &executor = read_state.executor;
	//! Make sure all tasks are done before shutting down
	executor.WorkOnTasks();
};

optional_ptr<const BoundIcebergManifestEntry> IcebergMultiFileList::GetDataFile(idx_t file_id,
                                                                                lock_guard<mutex> &guard) const {
	D_ASSERT(view_initialized);
	if (file_id < data_manifest_entries.size()) {
		//! Have we already scanned this data file and returned it? If so, return it
		return data_manifest_entries[file_id];
	}

	while (file_id >= data_manifest_entries.size()) {
		if (!TryGetNextBatch(guard)) {
			FinishScanTasks(guard);
			return nullptr;
		}

		auto &view_cursor = data_view_cursor;
		auto &current_batch = view_cursor.current_batch;
		auto &bound_manifest_list_entry = data_manifests[current_batch.manifest_list_entry_idx];
		auto &manifest_list_entry = bound_manifest_list_entry.entry;
		auto &manifest_entries = manifest_list_entry.GetManifestEntries();
		auto &manifest_file = manifest_list_entry.file;
		if (!data_manifest_matches[current_batch.manifest_list_entry_idx]) {
			view_cursor.current_batch_offset = current_batch.end_index;
		}
		for (; view_cursor.current_batch_offset < current_batch.end_index && file_id >= data_manifest_entries.size();
		     view_cursor.current_batch_offset++) {
			auto &manifest_entry = manifest_entries[view_cursor.current_batch_offset];
			auto &data_file = manifest_entry.data_file;
			auto entry_path = data_file.file_path;
			if (options.allow_moved_paths) {
				entry_path = IcebergUtils::GetFullPath(GetPath(), entry_path, fs);
			}
			shared_state->data_file_partition_info[entry_path] = data_file.partition_info;
			shared_state->data_file_partition_info[data_file.file_path] = data_file.partition_info;

			if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
				continue;
			}

			// Check whether current data file is filtered out.
			if (table_filters.HasFilters() &&
			    !FileMatchesFilter(manifest_file, manifest_entry, IcebergManifestContentType::DATA)) {
				// Note: FileMatches filter will log a message if the file is pruned
				//! Skip this file
				continue;
			}

			// Check whether current data file belongs to an unknown puffin file, skip if so.
			if (StringUtil::CIEquals(data_file.file_format, "puffin")) {
				//! Skip this file
				continue;
			}

			auto bound_entry = bound_manifest_list_entry.BindEntry(manifest_entry);
			data_manifest_entries.push_back(bound_entry);
		}
		if (view_cursor.current_batch_offset >= current_batch.end_index) {
			view_cursor.has_current_batch = false;
		}
	}
	return data_manifest_entries[file_id];
}

namespace {

bool ScanOrderCompare(const Value &v1, const Value &v2, OrderByStatistics stat_type) {
	return (stat_type == OrderByStatistics::MAX && v1 < v2) || (stat_type == OrderByStatistics::MIN && v1 > v2);
}

struct IcebergOrderEntry {
	idx_t entry_idx;
	Value lower;
	Value upper;
	idx_t count;
};

} // namespace

void IcebergMultiFileList::EnsureScanOrderApplied(lock_guard<mutex> &guard) const {
	if (!scan_order_options || scan_order_applied) {
		return;
	}
	scan_order_applied = true;

	auto &opts = *scan_order_options;
	//! Iceberg only stores reliable min/max for numeric/temporal columns; string bounds may be truncated.
	if (opts.column_type != OrderByColumnType::NUMERIC || opts.column_idx.HasChildren()) {
		return;
	}

	idx_t materialized = 0;
	while (GetDataFile(materialized, guard)) {
		materialized++;
	}
	if (data_manifest_entries.size() <= 1) {
		return;
	}

	auto &schema_columns = GetSchema().columns;
	idx_t schema_idx = opts.column_idx.GetPrimaryIndex();
	if (schema_idx >= schema_columns.size()) {
		return;
	}
	auto &order_column = *schema_columns[schema_idx];
	int32_t field_id = order_column.id;

	bool can_prune = opts.row_limit.IsValid();
	vector<IcebergOrderEntry> order_entries;
	order_entries.reserve(data_manifest_entries.size());
	for (idx_t i = 0; i < data_manifest_entries.size(); i++) {
		auto &data_file = data_manifest_entries[i].entry.data_file;
		auto lower_it = data_file.lower_bounds.find(field_id);
		auto upper_it = data_file.upper_bounds.find(field_id);
		if (lower_it == data_file.lower_bounds.end() || upper_it == data_file.upper_bounds.end()) {
			//! A file without usable bounds for the order column cannot be placed; leave order untouched.
			return;
		}
		//! lower/upper bounds are stored as raw Iceberg-encoded blobs; decode to typed Values before comparing.
		auto stats = IcebergPredicateStats::DeserializeBounds(lower_it->second, upper_it->second, order_column.name,
		                                                      order_column.type);
		if (!stats.lower_bound || !stats.upper_bound || stats.lower_bound->IsNull() || stats.upper_bound->IsNull()) {
			return;
		}
		auto null_it = data_file.null_value_counts.find(field_id);
		if (null_it == data_file.null_value_counts.end() || null_it->second > 0) {
			//! NULLs (or an omitted null count) interact with NULLS FIRST/LAST:
			//! reordering stays safe, limit pruning does not.
			can_prune = false;
		}
		order_entries.push_back(
		    {i, *stats.lower_bound, *stats.upper_bound, NumericCast<idx_t>(data_file.record_count)});
	}

	if (can_prune) {
		for (idx_t i = 0; i < delete_manifest_matches.size(); i++) {
			if (delete_manifest_matches[i]) {
				//! record_count is pre-delete; pruning by it would drop files that still hold live rows.
				can_prune = false;
				break;
			}
		}
	}

	const auto stat_type = opts.order_by;
	const bool ascending = opts.order_type == OrderType::ASCENDING;
	auto primary = [&](const IcebergOrderEntry &e) -> const Value & {
		return stat_type == OrderByStatistics::MAX ? e.upper : e.lower;
	};
	auto opposite = [&](const IcebergOrderEntry &e) -> const Value & {
		return stat_type == OrderByStatistics::MAX ? e.lower : e.upper;
	};

	std::stable_sort(order_entries.begin(), order_entries.end(),
	                 [&](const IcebergOrderEntry &a, const IcebergOrderEntry &b) {
		                 return ascending ? primary(a) < primary(b) : primary(b) < primary(a);
	                 });

	idx_t keep = order_entries.size();
	if (can_prune && opts.row_limit.GetIndex() > 0) {
		const idx_t row_limit = opts.row_limit.GetIndex();
		keep = 0;
		//! Keep the prefix [0..k): prune the rest once the kept files hold >= row_limit rows that are each
		//! guaranteed to outrank every remaining file. order_entries[k]'s primary bound is the best any pruned
		//! file can reach, so a kept file qualifies in full only if its opposite bound already beats it.
		for (idx_t k = 0; k < order_entries.size(); k++) {
			const Value &frontier = primary(order_entries[k]);
			idx_t guaranteed = 0;
			for (idx_t j = 0; j < k; j++) {
				if (!ScanOrderCompare(opposite(order_entries[j]), frontier, stat_type)) {
					guaranteed += order_entries[j].count;
				}
				if (guaranteed >= row_limit) {
					break;
				}
			}
			if (guaranteed >= row_limit) {
				break;
			}
			keep = k + 1;
		}
	}

	if (keep < order_entries.size()) {
		DUCKDB_LOG(context, IcebergLogType,
		           "Iceberg Scan Order Pushdown, kept %llu of %llu 'data_file's for ORDER BY LIMIT %llu", keep,
		           order_entries.size(), opts.row_limit.GetIndex());
	}

	vector<BoundIcebergManifestEntry> reordered;
	reordered.reserve(keep);
	for (idx_t i = 0; i < keep; i++) {
		reordered.push_back(data_manifest_entries[order_entries[i].entry_idx]);
	}
	data_manifest_entries = std::move(reordered);
}

OpenFileInfo IcebergMultiFileList::GetFileInternal(idx_t file_id, lock_guard<mutex> &guard) const {
	InitializeView(guard);
	StartDataManifestScan(guard);
	EnsureScanOrderApplied(guard);

	auto found_manifest_entry = GetDataFile(file_id, guard);
	if (!found_manifest_entry) {
		return OpenFileInfo();
	}

	const auto &bound_manifest_entry = *found_manifest_entry;
	auto &manifest_file = GetManifestFileForEntry(bound_manifest_entry, IcebergManifestContentType::DATA);
	auto &manifest_entry = bound_manifest_entry.entry;
	auto &data_file = manifest_entry.data_file;
	const auto &path = data_file.file_path;

	if (!StringUtil::CIEquals(data_file.file_format, "parquet")) {
		throw NotImplementedException("File format '%s' not supported, only supports 'parquet' currently",
		                              data_file.file_format);
	}

	string file_path = path;
	if (options.allow_moved_paths) {
		auto iceberg_path = GetPath();
		auto &fs = FileSystem::GetFileSystem(context);
		file_path = IcebergUtils::GetFullPath(iceberg_path, path, fs);
	}
	OpenFileInfo res(file_path);
	auto extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	extended_info->options["file_size"] = Value::UBIGINT(data_file.file_size_in_bytes);
	// files managed by Iceberg are never modified - we can keep them cached
	extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
	// etag / last modified time can be set to dummy values
	extended_info->options["etag"] = Value("");
	extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
	if (bound_manifest_entry.HasFirstRowId()) {
		extended_info->options["first_row_id"] = Value::BIGINT(bound_manifest_entry.GetFirstRowId());
	}
	extended_info->options["sequence_number"] = Value::BIGINT(manifest_entry.GetSequenceNumber(manifest_file));
	res.extended_info = extended_info;
	return res;
}

OpenFileInfo IcebergMultiFileList::GetFile(idx_t file_id) const {
	lock_guard<mutex> guard(shared_state->lock);
	return GetFileInternal(file_id, guard);
}

bool IcebergMultiFileList::ManifestMatchesFilter(const IcebergManifestFile &manifest) const {
	auto spec_id = manifest.partition_spec_id;
	auto &metadata = GetMetadata();

	auto partition_spec_it = metadata.partition_specs.find(spec_id);
	if (partition_spec_it == metadata.partition_specs.end()) {
		throw InvalidInputException("Manifest %s references 'partition_spec_id' %d which doesn't exist",
		                            manifest.manifest_path, spec_id);
	}
	auto &partition_spec = partition_spec_it->second;
	if (!manifest.partitions.has_partitions) {
		//! No field summaries are present, can't filter anything
		return true;
	}

	auto &field_summaries = manifest.partitions.field_summary;
	if (partition_spec.fields.size() != field_summaries.size()) {
		throw InvalidInputException(
		    "Manifest has %d 'field_summary' entries but the referenced partition spec has %d fields",
		    field_summaries.size(), partition_spec.fields.size());
	}

	if (!table_filters.HasFilters()) {
		//! There are no filters
		return true;
	}

	auto &schema = GetSchema().columns;
	unordered_map<uint64_t, ColumnIndex> source_to_column_id;
	IcebergTableSchema::PopulateSourceIdMap(source_to_column_id, schema, nullptr);

	for (idx_t i = 0; i < field_summaries.size(); i++) {
		auto &field_summary = field_summaries[i];
		auto &field = partition_spec.fields[i];

		const auto &column_id = source_to_column_id.at(field.source_id);

		// Find if we have a filter for this source column
		auto table_filter = GetFilterForColumnIndex(table_filters, column_id);
		if (!table_filter) {
			continue;
		}

		auto &column = IcebergTableSchema::GetFromColumnIndex(schema, column_id, 0);
		auto result_type = field.transform.GetSerializedType(column.type);
		auto stats = IcebergPredicateStats::DeserializeBounds(field_summary.lower_bound, field_summary.upper_bound,
		                                                      column.name, result_type);
		stats.has_nan = field_summary.contains_nan;
		stats.has_null = field_summary.contains_null;
		stats.has_not_null = true; // Not enough information in field_summary to determine if this should be false

		if (!IcebergPredicate::MatchBounds(context, *table_filter, stats, field.transform)) {
			DUCKDB_LOG(context, IcebergLogType,
			           "Iceberg Filter Pushdown, skipped 'manifest_file': '%s', column '%s' with "
			           "transform '%s', bounds [%s, %s] did not match filter: %s",
			           manifest.manifest_path, column.name, field.transform.RawType(),
			           stats.lower_bound ? stats.lower_bound->ToString() : "N/A",
			           stats.upper_bound ? stats.upper_bound->ToString() : "N/A", table_filter->ToString(column.name));
			return false;
		}
	}
	return true;
}

vector<reference<const IcebergEqualityDeleteFile>>
IcebergMultiFileList::GetEqualityDeletesForFile(const BoundIcebergManifestEntry &bound_manifest_entry) const {
	lock_guard<mutex> guard(shared_state->delete_lock);
	vector<reference<const IcebergEqualityDeleteFile>> result;

	//! Look through all the equality delete files with a *higher* sequence number
	auto &manifest_entry = bound_manifest_entry.entry;
	auto &manifest_file = data_manifests[bound_manifest_entry.manifest_file_idx].entry.file;
	auto &data_file = manifest_entry.data_file;
	auto &metadata = GetMetadata();
	auto it = shared_state->equality_delete_data.upper_bound(manifest_entry.GetSequenceNumber(manifest_file));
	for (; it != shared_state->equality_delete_data.end(); it++) {
		auto &files = it->second->files;
		for (auto &file : files) {
			auto &partition_spec = metadata.partition_specs.at(file.partition_spec_id);
			if (partition_spec.IsPartitioned()) {
				if (file.partition_spec_id != manifest_file.partition_spec_id) {
					//! Not unpartitioned and the data does not share the same partition spec as the delete, skip the
					//! delete file.
					continue;
				}
				D_ASSERT(file.partition_info.size() == data_file.partition_info.size());
				for (idx_t i = 0; i < file.partition_info.size(); i++) {
					if (file.partition_info[i] != data_file.partition_info[i]) {
						//! Same partition spec id, but the partitioning information doesn't match, delete file doesn't
						//! apply.
						continue;
					}
				}
			}
			result.emplace_back(file);
		}
	}
	return result;
}

void IcebergMultiFileList::InitializeView(lock_guard<mutex> &guard) const {
	if (view_initialized) {
		return;
	}
	LoadManifestList(guard);

	auto &committed_data_manifests = shared_state->committed_data_manifests;
	auto &transaction_data_manifests = shared_state->transaction_data_manifests;
	data_manifests.reserve(committed_data_manifests.size() + transaction_data_manifests.size());
	data_manifest_matches.reserve(committed_data_manifests.size() + transaction_data_manifests.size());
	for (auto &manifest : committed_data_manifests) {
		data_manifests.emplace_back(data_manifests.size(), manifest);
		data_manifest_matches.push_back(ManifestMatchesFilter(manifest.file));
	}
	for (auto &manifest : transaction_data_manifests) {
		data_manifests.emplace_back(data_manifests.size(), manifest);
		data_manifest_matches.push_back(ManifestMatchesFilter(manifest.get().file));
	}

	auto &committed_delete_manifests = shared_state->committed_delete_manifests;
	auto &transaction_delete_manifests = shared_state->transaction_delete_manifests;
	delete_manifests.reserve(committed_delete_manifests.size() + transaction_delete_manifests.size());
	delete_manifest_matches.reserve(committed_delete_manifests.size() + transaction_delete_manifests.size());
	for (auto &manifest : committed_delete_manifests) {
		delete_manifests.emplace_back(delete_manifests.size(), manifest);
		delete_manifest_matches.push_back(ManifestMatchesFilter(manifest.file));
	}
	for (auto &manifest : transaction_delete_manifests) {
		delete_manifests.emplace_back(delete_manifests.size(), manifest);
		delete_manifest_matches.push_back(ManifestMatchesFilter(manifest.get().file));
	}
	view_initialized = true;
}

void IcebergMultiFileList::LoadManifestList(lock_guard<mutex> &guard) const {
	if (shared_state->manifest_list_loaded) {
		return;
	}

	auto &snapshot_info = shared_state->scan_info->snapshot_info;
	if (snapshot_info.snapshot) {
		//! Load the snapshot
		auto iceberg_path = GetPath();
		auto &snapshot_info = GetSnapshot();
		auto &snapshot = *snapshot_info.snapshot;
		auto &metadata = GetMetadata();
		auto &fs = FileSystem::GetFileSystem(context);

		vector<IcebergManifestListEntry> manifest_list_entries;
		if (HasTransactionData() && !GetTransactionData().alters.empty()) {
			auto &transaction_data = GetTransactionData();
			manifest_list_entries = transaction_data.existing_manifest_list;
		} else {
			// Read the manifest list, we need all the manifests to determine if we've seen all deletes
			auto manifest_list_full_path = options.allow_moved_paths
			                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
			                                   : snapshot.manifest_list;
			//! Read the manifest list
			auto scan = AvroScan::ScanManifestList(snapshot_info, metadata, context, manifest_list_full_path,
			                                       manifest_list_entries);
			auto manifest_list_reader = make_uniq<manifest_list::ManifestListReader>(*scan);
			while (!manifest_list_reader->Finished()) {
				manifest_list_reader->Read();
			}
		}

		for (auto &manifest_list_entry : manifest_list_entries) {
			auto &manifest_file = manifest_list_entry.file;
			if (manifest_file.content == IcebergManifestContentType::DATA) {
				shared_state->committed_data_manifests.push_back(std::move(manifest_list_entry));
			} else {
				D_ASSERT(manifest_file.content == IcebergManifestContentType::DELETE);
				shared_state->committed_delete_manifests.push_back(std::move(manifest_list_entry));
			}
		}

		for (auto &manifest : shared_state->committed_data_manifests) {
			if (!manifest.HasManifestEntries()) {
				continue;
			}
			auto &file = manifest.file;
			idx_t reserve_size = file.existing_files_count + file.added_files_count + file.deleted_files_count;
			manifest.GetManifestEntries().reserve(reserve_size);
		}
	}

	if (HasTransactionData()) {
		auto &transaction_data = GetTransactionData();
		for (auto &alter_p : transaction_data.alters) {
			auto &alter = alter_p.get();
			const auto &manifest_list_entries = alter.GetManifestFiles();
			for (auto &manifest_list_entry : manifest_list_entries) {
				auto &manifest = manifest_list_entry.file;
				switch (manifest.content) {
				case IcebergManifestContentType::DATA: {
					shared_state->transaction_data_manifests.push_back(manifest_list_entry);
					break;
				}
				case IcebergManifestContentType::DELETE: {
					shared_state->transaction_delete_manifests.push_back(manifest_list_entry);
					break;
				}
				default:
					throw NotImplementedException("IcebergManifestContentType: %d",
					                              static_cast<uint8_t>(manifest.content));
				}
			}
		}
	}

	shared_state->manifest_list_loaded = true;
	DUCKDB_LOG(context, IcebergLogType,
	           "Iceberg metadata phase=manifest_list_loaded data_manifests=%llu delete_manifests=%llu",
	           shared_state->committed_data_manifests.size() + shared_state->transaction_data_manifests.size(),
	           shared_state->committed_delete_manifests.size() + shared_state->transaction_delete_manifests.size());
}

void IcebergMultiFileList::StartDeleteManifestScan() const {
	if (shared_state->delete_manifest_scan || shared_state->committed_delete_manifests.empty()) {
		return;
	}

	auto &snapshot_info = GetSnapshot();
	auto &metadata = GetMetadata();
	auto iceberg_path = GetPath();
	auto &fs = FileSystem::GetFileSystem(context);
	shared_state->delete_manifest_scan = AvroScan::ScanManifest(snapshot_info, shared_state->committed_delete_manifests,
	                                                            options, fs, iceberg_path, metadata, context);
	shared_state->delete_manifest_reader =
	    make_uniq<manifest_file::ManifestReader>(*shared_state->delete_manifest_scan);
}

void IcebergMultiFileList::StartDataManifestScan(lock_guard<mutex> &guard) const {
	D_ASSERT(view_initialized);
	if (shared_state->data_manifest_scan_started) {
		//! Filter pushdown creates multiple list views that share this state. Execution can still call into an older,
		//! less-filtered view after the filtered view selected and started the manifest scan. The first execution view
		//! owns the selection: later views consume the same batches instead of broadening the scan again.
		return;
	}

	shared_state->data_manifest_scan_started = true;

	const auto committed_manifest_count = shared_state->committed_data_manifests.size();
	vector<idx_t> selected_committed_manifests;
	for (idx_t manifest_idx = 0; manifest_idx < committed_manifest_count; manifest_idx++) {
		if (data_manifest_matches[manifest_idx]) {
			selected_committed_manifests.push_back(manifest_idx);
		}
	}

	for (idx_t transaction_idx = 0; transaction_idx < shared_state->transaction_data_manifests.size();
	     transaction_idx++) {
		auto manifest_idx = committed_manifest_count + transaction_idx;
		if (!data_manifest_matches[manifest_idx]) {
			continue;
		}
		auto &manifest = shared_state->transaction_data_manifests[transaction_idx].get();
		shared_state->read_state.PushBatch(ManifestReadBatch {manifest_idx, 0, manifest.GetManifestEntries().size()});
	}

	if (!selected_committed_manifests.empty()) {
		auto &metadata = GetMetadata();
		auto &snapshot_info = GetSnapshot();
		auto iceberg_path = GetPath();
		auto &fs = FileSystem::GetFileSystem(context);

		auto data_scan =
		    AvroScan::ScanManifest(snapshot_info, shared_state->committed_data_manifests, options, fs, iceberg_path,
		                           metadata, context, &shared_state->read_state, selected_committed_manifests);
		shared_state->data_manifest_read_state = make_uniq<IcebergManifestScanningState>(
		    context, std::move(data_scan), shared_state->committed_data_manifests);

		auto &executor = shared_state->data_manifest_read_state->executor;
		auto &scheduler = TaskScheduler::GetScheduler(context);
		auto worker_thread_count = scheduler.NumberOfThreads();

		auto num_threads = MinValue<idx_t>(worker_thread_count, selected_committed_manifests.size());
		shared_state->data_manifest_read_state->in_progress_tasks = num_threads;
		for (idx_t i = 0; i < num_threads; i++) {
			executor.ScheduleTask(make_uniq<ManifestReadTask>(*shared_state->data_manifest_read_state));
		}
	}

	idx_t selected_manifest_count = 0;
	for (auto matches : data_manifest_matches) {
		selected_manifest_count += matches;
	}
	DUCKDB_LOG(context, IcebergLogType,
	           "Iceberg metadata phase=data_manifest_scan_started selected_data_manifests=%llu "
	           "total_data_manifests=%llu filters=%llu",
	           selected_manifest_count, data_manifest_matches.size(), table_filters.FilterCount());
}

void IcebergMultiFileList::EnumerateDeleteManifestEntriesInternal() const {
	if (shared_state->delete_entries_enumerated) {
		return;
	}
	StartDeleteManifestScan();

	// In <=v2 we now have to process *all* delete manifests
	// before we can be certain that we have all the delete data for the current file.

	// v3 solves this, `referenced_data_file` will tell us which file the `data_file`
	// is targeting before we open it, and there can only be one deletion vector per data file.

	// From the spec: "At most one deletion vector is allowed per data file in a snapshot"
	optional_ptr<const case_insensitive_map_t<string>> transactional_delete_files;
	if (HasTransactionData()) {
		transactional_delete_files = GetTransactionData().transactional_delete_files;
	}
	while (!FinishedScanningDeletes()) {
		shared_state->delete_manifest_reader->Read();
	}

	for (idx_t i = 0; i < shared_state->committed_delete_manifests.size(); i++) {
		auto &manifest_list_entry = shared_state->committed_delete_manifests[i];
		auto manifest = BoundIcebergManifestListEntry(i, manifest_list_entry);
		for (auto &manifest_entry : manifest_list_entry.GetManifestEntries()) {
			if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
				continue;
			}
			auto &data_file = manifest_entry.data_file;
			auto &referenced_data_file = data_file.referenced_data_file;
			if (referenced_data_file && transactional_delete_files &&
			    transactional_delete_files->count(*referenced_data_file)) {
				//! Skip this delete file, there's a transaction-local delete that makes it obsolete
				continue;
			}
			auto bound_entry = manifest.BindEntry(manifest_entry);
			shared_state->delete_manifest_entries.push_back(std::move(bound_entry));
		}
	}

	auto offset = shared_state->committed_delete_manifests.size();
	for (idx_t transaction_delete_idx = 0; transaction_delete_idx < shared_state->transaction_delete_manifests.size();
	     transaction_delete_idx++) {
		auto &manifest_list_entry = shared_state->transaction_delete_manifests[transaction_delete_idx].get();
		auto delete_manifest = BoundIcebergManifestListEntry(offset + transaction_delete_idx, manifest_list_entry);
		for (auto &manifest_entry : manifest_list_entry.GetManifestEntries()) {
			auto &data_file = manifest_entry.data_file;
			auto &referenced_data_file = data_file.referenced_data_file;
			if (referenced_data_file && transactional_delete_files) {
				auto it = transactional_delete_files->find(*referenced_data_file);
				//! Check if this is the currently active (last) delete file for this referenced_data_file
				if (it != transactional_delete_files->end() && it->second != data_file.file_path) {
					continue;
				}
			}
			//! FIXME: no file pruning for uncommitted data?
			auto bound_manifest_entry = delete_manifest.BindEntry(manifest_entry);
			shared_state->delete_manifest_entries.push_back(std::move(bound_manifest_entry));
		}
	}

	shared_state->delete_entries_enumerated = true;
	D_ASSERT(FinishedScanningDeletes());
}

void IcebergMultiFileList::ScanDeleteFiles(const vector<MultiFileColumnDefinition> &global_columns,
                                           const vector<ColumnIndex> &global_column_ids,
                                           const vector<idx_t> &projection_ids) const {
	for (; shared_state->next_delete_entry_to_process < shared_state->delete_manifest_entries.size();
	     shared_state->next_delete_entry_to_process++) {
		auto &bound_manifest_entry = shared_state->delete_manifest_entries[shared_state->next_delete_entry_to_process];
		auto &manifest_entry = bound_manifest_entry.entry;
		auto &data_file = manifest_entry.data_file;
		if (StringUtil::CIEquals(data_file.file_format, "parquet")) {
			ScanDeleteFile(bound_manifest_entry, global_columns, global_column_ids, projection_ids);
		} else if (StringUtil::CIEquals(data_file.file_format, "puffin")) {
			ScanPuffinFile(bound_manifest_entry);
		} else {
			throw NotImplementedException(
			    "File format '%s' not supported for deletes, only supports 'parquet' and 'puffin' currently",
			    data_file.file_format);
		}
	}
}

void IcebergMultiFileList::ProcessDeletes(const vector<MultiFileColumnDefinition> &global_columns,
                                          const vector<ColumnIndex> &global_column_ids,
                                          const vector<idx_t> &projection_ids) const {
	lock_guard<mutex> guard(shared_state->lock);
	InitializeView(guard);
	lock_guard<mutex> delete_guard(shared_state->delete_lock);
	ProcessDeletesInternal(global_columns, global_column_ids, projection_ids);
}

void IcebergMultiFileList::ProcessDeletesInternal(const vector<MultiFileColumnDefinition> &global_columns,
                                                  const vector<ColumnIndex> &global_column_ids,
                                                  const vector<idx_t> &projection_ids) const {
	//! Enumerate the delete manifest entries, then read the delete files they reference.
	//! Delete enumeration is idempotent, so this is safe even if the entries were
	//! already enumerated earlier (e.g. by the optimizer).
	EnumerateDeleteManifestEntriesInternal();
	ScanDeleteFiles(global_columns, global_column_ids, projection_ids);
}

vector<BoundIcebergManifestEntry> IcebergMultiFileList::GetDeleteManifestEntries() const {
	lock_guard<mutex> guard(shared_state->lock);
	InitializeView(guard);
	lock_guard<mutex> delete_guard(shared_state->delete_lock);
	EnumerateDeleteManifestEntriesInternal();
	vector<BoundIcebergManifestEntry> result;
	for (auto &entry : shared_state->delete_manifest_entries) {
		auto manifest_idx = entry.manifest_file_idx;
		auto &manifest = delete_manifests[manifest_idx];
		auto &manifest_file = manifest.entry.file;
		if (!delete_manifest_matches[manifest_idx]) {
			continue;
		}
		if (table_filters.HasFilters() &&
		    !FileMatchesFilter(manifest_file, entry.entry, IcebergManifestContentType::DELETE)) {
			continue;
		}
		result.push_back(entry);
	}
	return result;
}

void IcebergMultiFileList::ScanDeleteFile(const BoundIcebergManifestEntry &bound_manifest_entry,
                                          const vector<MultiFileColumnDefinition> &global_columns,
                                          const vector<ColumnIndex> &global_column_ids,
                                          const vector<idx_t> &projection_ids) const {
	auto &manifest_entry = bound_manifest_entry.entry;
	auto &data_file = manifest_entry.data_file;
	auto delete_file_path = data_file.file_path;
	auto iceberg_deletes_scan = IcebergFunctions::GetIcebergDeletesScanFunction(context);
	auto &delete_scan_function = iceberg_deletes_scan.functions[0];

	if (options.allow_moved_paths) {
		auto iceberg_path = GetPath();
		auto &fs = FileSystem::GetFileSystem(context);
		delete_file_path = IcebergUtils::GetFullPath(iceberg_path, delete_file_path, fs);
	}
	// Prepare the inputs for the bind
	vector<Value> children;
	children.reserve(1);
	children.push_back(Value(delete_file_path));
	named_parameter_map_t named_params;
	vector<LogicalType> input_types;
	vector<Identifier> input_names;

	TableFunctionRef empty;
	OpenFileInfo res(delete_file_path);
	// create function info for the iceberg delete scan.
	auto extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	extended_info->options["file_size"] = Value::UBIGINT(data_file.file_size_in_bytes);
	// files managed by Iceberg are never modified - we can keep them cached
	extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
	// etag / last modified time can be set to dummy values
	extended_info->options["etag"] = Value("");
	extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
	res.extended_info = extended_info;
	auto delete_info = make_shared_ptr<IcebergDeleteScanInfo>(res);
	delete_scan_function.function_info = delete_info;

	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  delete_scan_function, empty);
	vector<LogicalType> return_types;
	vector<string> return_names;
	auto bind_data = delete_scan_function.bind(context, bind_input, return_types, return_names);

	DataChunk result;
	// Reserve for STANDARD_VECTOR_SIZE instead of count, in case the returned table contains too many tuples
	result.Initialize(context, return_types, STANDARD_VECTOR_SIZE);

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);

	vector<column_t> column_ids;
	for (idx_t i = 0; i < return_types.size(); i++) {
		column_ids.push_back(i);
	}
	TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	auto global_state = delete_scan_function.init_global(context, input);
	auto local_state = delete_scan_function.init_local(execution_context, input, global_state.get());

	auto &multi_file_local_state = local_state->Cast<MultiFileLocalState>();

	if (data_file.content == IcebergManifestEntryContentType::POSITION_DELETES) {
		do {
			TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
			result.Reset();
			delete_scan_function.function(context, function_input, result);
			result.Flatten();
			ScanPositionalDeleteFile(bound_manifest_entry, result);
		} while (result.size() != 0);
	} else if (data_file.content == IcebergManifestEntryContentType::EQUALITY_DELETES) {
		do {
			TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
			result.Reset();
			delete_scan_function.function(context, function_input, result);
			result.Flatten();
			ScanEqualityDeleteFile(bound_manifest_entry, result, multi_file_local_state.job.reader->columns,
			                       global_columns, global_column_ids, projection_ids);
		} while (result.size() != 0);
	}
}

unique_ptr<DeleteFilter> IcebergMultiFileList::GetPositionalDeletesForFile(const string &file_path) const {
	lock_guard<mutex> guard(shared_state->delete_lock);
	auto it = shared_state->positional_delete_data.find(file_path);
	if (it != shared_state->positional_delete_data.end()) {
		// There is delete data for this file, return it
		return it->second->ToFilter();
	}
	return nullptr;
}

shared_ptr<IcebergDeleteData> IcebergMultiFileList::GetExistingPositionalDeleteData(const string &file_path) const {
	lock_guard<mutex> guard(shared_state->delete_lock);
	auto it = shared_state->positional_delete_data.find(file_path);
	if (it == shared_state->positional_delete_data.end()) {
		return nullptr;
	}
	return it->second;
}

} // namespace duckdb
