#include "planning/iceberg_multi_file_list.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
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

#include "planning/iceberg_multi_file_reader.hpp"
#include "function/iceberg_functions.hpp"
#include "planning/metadata_io/deletes/iceberg_deletes_file_reader.hpp"
#include "common/iceberg_utils.hpp"
#include "iceberg_logging.hpp"
#include "planning/pruning/iceberg_predicate.hpp"
#include "core/expression/iceberg_value.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "core/expression/iceberg_predicate_stats.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/metadata_io/manifest_list/iceberg_manifest_list_reader.hpp"
#include "planning/metadata_io/manifest_list/bound_iceberg_manifest_list_entry.hpp"

namespace duckdb {

void ManifestEntryReadState::PushBatch(ManifestReadBatch &&batch) {
	lock_guard<mutex> guard(lock);
	batches.push(std::move(batch));
}

bool ManifestEntryReadState::HasCurrentBatch() const {
	return has_batch;
}

optional_ptr<ManifestReadBatch> ManifestEntryReadState::GetCurrentBatch() {
	if (!has_batch) {
		lock_guard<mutex> guard(lock);
		if (batches.empty()) {
			return nullptr;
		}
		auto &batch = batches.front();
		current_batch = std::move(batch);
		batches.pop();
		has_batch = true;
	}
	return current_batch;
}

void ManifestEntryReadState::FinishBatch() {
	has_batch = false;
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
		if (!TryGetStructExtractChildIndex(func, child_idx) || func.children.empty()) {
			return false;
		}
		if (!TryGetFilterPath(*func.children[0], path)) {
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
		if (func.function.GetName() == OptionalFilterScalarFun::NAME && func.bind_info) {
			auto &data = func.bind_info->Cast<OptionalFilterFunctionData>();
			return data.child_filter_expr ? ExtractFilterExpressionForPath(*data.child_filter_expr, path) : nullptr;
		}
		if (func.function.GetName() == SelectivityOptionalFilterScalarFun::NAME && func.bind_info) {
			auto &data = func.bind_info->Cast<SelectivityOptionalFilterFunctionData>();
			return data.child_filter_expr ? ExtractFilterExpressionForPath(*data.child_filter_expr, path) : nullptr;
		}
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		auto result = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &child : conjunction.children) {
			auto extracted_child = ExtractFilterExpressionForPath(*child, path);
			if (extracted_child) {
				result->children.push_back(std::move(extracted_child));
			}
		}
		if (result->children.empty()) {
			return nullptr;
		}
		if (result->children.size() == 1) {
			return std::move(result->children[0]);
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
    : context(context_p), fs(FileSystem::GetFileSystem(context)), scan_info(scan_info), path(path), table(nullptr),
      options(options) {
}

IcebergMultiFileList::~IcebergMultiFileList() {
	lock_guard<mutex> guard(lock);
	//! FIXME: this could throw, if the tasks encountered an error
	FinishScanTasks(guard);
}

string IcebergMultiFileList::ToDuckDBPath(const string &raw_path) {
	return raw_path;
}

string IcebergMultiFileList::GetPath() const {
	return path;
}

const IcebergTableMetadata &IcebergMultiFileList::GetMetadata() const {
	return scan_info->metadata;
}

bool IcebergMultiFileList::HasTransactionData() const {
	return scan_info->transaction_data;
}

const IcebergTransactionData &IcebergMultiFileList::GetTransactionData() const {
	D_ASSERT(HasTransactionData());
	return *scan_info->transaction_data;
}

const IcebergSnapshotScanInfo &IcebergMultiFileList::GetSnapshot() const {
	return scan_info->snapshot_info;
}

const IcebergTableSchema &IcebergMultiFileList::GetSchema() const {
	return scan_info->schema;
}

bool IcebergMultiFileList::FinishedScanningDeletes() const {
	return !delete_manifest_reader || delete_manifest_reader->Finished();
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

void IcebergMultiFileList::Bind(vector<LogicalType> &return_types, vector<string> &names) {
	lock_guard<mutex> guard(lock);

	if (have_bound) {
		names = this->names;
		return_types = this->types;
		return;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	auto caching_fs = make_shared_ptr<CachingFileSystemWrapper>(FileSystem::GetFileSystem(context), *context.db);
	if (!scan_info) {
		D_ASSERT(!path.empty());
		auto input_string = path;
		auto iceberg_path = IcebergUtils::GetStorageLocation(context, input_string);
		auto iceberg_meta_path = IcebergTableMetadata::GetMetaDataPath(context, iceberg_path, fs, options);
		auto table_metadata =
		    IcebergTableMetadata::Parse(iceberg_meta_path, *caching_fs, options.metadata_compression_codec);

		auto temp_data = make_uniq<IcebergScanTemporaryData>();
		temp_data->metadata = IcebergTableMetadata::FromTableMetadata(table_metadata);
		auto &metadata = temp_data->metadata;

		IcebergSnapshotScanInfo snapshot_info;
		snapshot_info = metadata.GetSnapshot(options.snapshot_lookup);
		auto schema = metadata.GetSchemaFromId(snapshot_info.schema_id);
		scan_info = make_shared_ptr<IcebergScanInfo>(iceberg_path, std::move(temp_data), snapshot_info, *schema);
	}

	if (!initialized) {
		InitializeFiles(guard);
	}

	auto &schema = GetSchema().columns;
	for (auto &schema_entry : schema) {
		names.push_back(schema_entry->name);
		return_types.push_back(schema_entry->type);
	}

	QueryResult::DeduplicateColumns(names);
	for (idx_t i = 0; i < names.size(); i++) {
		schema[i]->name = names[i];
	}

	have_bound = true;
	this->names = names;
	this->types = return_types;
}

unique_ptr<IcebergMultiFileList> IcebergMultiFileList::PushdownInternal(ClientContext &context,
                                                                        TableFilterSet &new_filters,
                                                                        vector<column_t> column_indexes) const {
	auto filtered_list = make_uniq<IcebergMultiFileList>(context, scan_info, path, this->options);

	IcebergTableFilters result_filter_set;

	// Add pre-existing filters
	for (auto &entry : table_filters) {
		result_filter_set.PushFilter(entry.first, entry.second->Copy());
	}

	// Add new filters
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
	return filtered_list;
}

unique_ptr<MultiFileList>
IcebergMultiFileList::DynamicFilterPushdown(ClientContext &context, const MultiFileOptions &options,
                                            const vector<string> &names, const vector<LogicalType> &types,
                                            const vector<column_t> &column_ids, TableFilterSet &filters) const {
	if (!filters.HasFilters()) {
		return nullptr;
	}

	TableFilterSet filters_copy;
	for (auto &entry : filters) {
		auto &filter =
		    ExpressionFilter::GetExpressionFilter(entry.Filter(), "IcebergMultiFileList::DynamicFilterPushdown");
		auto column_id = column_ids[entry.GetIndex().GetIndex()];
		auto previously_pushed_down_filter = table_filters.TryGetFilterByColumnIndex(column_id);
		if (previously_pushed_down_filter && filter.Equals(*previously_pushed_down_filter)) {
			// Skip filters that we already have pushed down
			continue;
		}
		filters_copy.PushFilter(entry.GetIndex(), filter.Copy());
	}

	if (filters_copy.HasFilters()) {
		auto new_snap = PushdownInternal(context, filters_copy, column_ids);
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
	lock_guard<mutex> guard(lock);
	for (idx_t i = 0; i < data_manifest_entries.size(); i++) {
		file_list.push_back(GetFileInternal(i, guard));
	}
	return file_list;
}

FileExpandResult IcebergMultiFileList::GetExpandResult() const {
	// GetFileInternal(1) will ensure files with index 0 and index 1 are expanded if they are available
	lock_guard<mutex> guard(lock);
	GetFileInternal(1, guard);

	// always return multiple files, In the case there is only 1 data file,
	// we only lose performance if it is small
	return FileExpandResult::MULTIPLE_FILES;
}

idx_t IcebergMultiFileList::GetTotalFileCount() const {
	// FIXME: the 'added_files_count' + the 'existing_files_count'
	// in the Manifest List should give us this information without scanning the manifest file(s)
	lock_guard<mutex> guard(lock);

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

	//! Make sure we have fetched all manifests
	(void)GetTotalFileCount();
	D_ASSERT(initialized);

	idx_t cardinality = 0;
	for (idx_t i = 0; i < data_manifests.size(); i++) {
		auto &manifest = data_manifests[i].entry.file;
		cardinality += manifest.added_rows_count;
		cardinality += manifest.existing_rows_count;
	}
	for (idx_t i = 0; i < delete_manifests.size(); i++) {
		auto &manifest = delete_manifests[i].entry.file;
		cardinality -= manifest.added_rows_count;
	}
	return make_uniq<NodeStatistics>(cardinality, cardinality);
}

const BoundIcebergManifestEntry &IcebergMultiFileList::GetManifestEntry(idx_t file_id) const {
	return data_manifest_entries[file_id];
}

vector<IcebergPartitionInfo> IcebergMultiFileList::GetPartitionInfoForDataFile(const string &file_path) const {
	lock_guard<mutex> guard(lock);
	auto iceberg_path = GetPath();
	for (auto &bound_entry : data_manifest_entries) {
		auto &data_file = bound_entry.entry.data_file;
		string entry_path = data_file.file_path;
		if (options.allow_moved_paths) {
			entry_path = IcebergUtils::GetFullPath(iceberg_path, entry_path, fs);
		}
		if (StringUtil::CIEquals(entry_path, file_path)) {
			return data_file.partition_info;
		}
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

unique_ptr<BaseStatistics> IcebergPartitionRowGroup::GetColumnStatistics(const StorageIndex &storage_index) {
	auto col_idx = storage_index.GetPrimaryIndex();
	if (col_idx >= schema.size()) {
		return nullptr;
	}
	auto &column = *schema[col_idx];
	auto field_id = column.id;

	auto lower_it = data_file.lower_bounds.find(field_id);
	auto upper_it = data_file.upper_bounds.find(field_id);
	if (lower_it == data_file.lower_bounds.end() && upper_it == data_file.upper_bounds.end()) {
		return nullptr;
	}

	Value lower_bound, upper_bound;
	if (lower_it != data_file.lower_bounds.end()) {
		lower_bound = lower_it->second;
	}
	if (upper_it != data_file.upper_bounds.end()) {
		upper_bound = upper_it->second;
	}

	// DeserializeBounds converts from BLOB to typed values
	IcebergPredicateStats pred_stats;
	try {
		pred_stats = IcebergPredicateStats::DeserializeBounds(lower_bound, upper_bound, column.name, column.type);
	} catch (...) {
		return nullptr;
	}

	if (!pred_stats.has_lower_bounds && !pred_stats.has_upper_bounds) {
		return nullptr;
	}

	auto &type = column.type;
	if (type.IsNumeric() || type.IsTemporal()) {
		auto stats = NumericStats::CreateEmpty(type);
		if (pred_stats.has_lower_bounds) {
			NumericStats::SetMin(stats, pred_stats.lower_bound);
		}
		if (pred_stats.has_upper_bounds) {
			NumericStats::SetMax(stats, pred_stats.upper_bound);
		}
		return stats.ToUnique();
	} else if (type.id() == LogicalTypeId::VARCHAR) {
		auto stats = StringStats::CreateEmpty(type);
		if (pred_stats.has_lower_bounds) {
			StringStats::SetMin(stats, pred_stats.lower_bound.GetValueUnsafe<string_t>());
		}
		if (pred_stats.has_upper_bounds) {
			StringStats::SetMax(stats, pred_stats.upper_bound.GetValueUnsafe<string_t>());
		}
		return stats.ToUnique();
	}
	return nullptr;
}

bool IcebergPartitionRowGroup::MinMaxIsExact(const BaseStatistics &stats, const StorageIndex &storage_index) {
	// File-level bounds are not exact (they are min/max across the file, not single values)
	// This is fine for partition pruning but not for eager MIN/MAX aggregates
	return false;
}

void IcebergMultiFileList::GetStatistics(vector<PartitionStatistics> &result) const {
	if (GetMetadata().iceberg_version == 1) {
		//! We collect no statistics information from manifests for V1 tables.
		return;
	}

	lock_guard<mutex> guard(lock);
	if (!initialized) {
		InitializeFiles(guard);
	}

	//lock_guard<mutex> delete_guard(delete_lock);
	//if (!FinishedScanningDeletes() || transaction_delete_idx < transaction_delete_manifests.size()) {
	//	ProcessDeletes(global_columns, global_column_ids);
	//}

	auto &schema = GetSchema().columns;

	//! Iterate through the data
	for (auto &bound_entry : data_manifest_entries) {
		auto &entry = bound_entry.entry;
		auto &data_file = entry.data_file;

		idx_t deleted_rows = 0;
		bool count_is_exact = true;
		auto it = positional_delete_data.find(data_file.file_path);
		if (it != positional_delete_data.end()) {
			auto &delete_data = *it->second;
			if (delete_data.type != IcebergDeleteType::DELETION_VECTOR) {
				count_is_exact = false;
			}
			for (auto &bound_delete_entry : delete_data.entries) {
				auto &delete_entry = bound_delete_entry.entry;
				deleted_rows += delete_entry.data_file.record_count;
			}
		}

		auto equality_deletes = GetEqualityDeletesForFile(bound_entry);
		if (!equality_deletes.empty()) {
			//! No clue how many rows are deleted
			count_is_exact = false;
		}

		PartitionStatistics stats;
		stats.count = NumericCast<idx_t>(data_file.record_count - deleted_rows);
		stats.count_type = count_is_exact ? CountType::COUNT_EXACT : CountType::COUNT_APPROXIMATE;
		if (!data_file.lower_bounds.empty() || !data_file.upper_bounds.empty()) {
			stats.partition_row_group = make_shared_ptr<IcebergPartitionRowGroup>(schema, data_file);
		}
		result.push_back(std::move(stats));
	}
}

void IcebergPredicateStats::SetLowerBound(const Value &new_lower_bound) {
	has_lower_bounds = true;
	lower_bound = new_lower_bound;
}

void IcebergPredicateStats::SetUpperBound(const Value &new_upper_bound) {
	has_upper_bounds = true;
	upper_bound = new_upper_bound;
}

bool IcebergPredicateStats::BoundsAreNull() const {
	return has_lower_bounds && has_upper_bounds && lower_bound.IsNull() && upper_bound.IsNull();
}

IcebergPredicateStats IcebergPredicateStats::DeserializeBounds(const Value &lower_bound, const Value &upper_bound,
                                                               const string &name, const LogicalType &type) {
	IcebergPredicateStats res;

	// DuckDB-Iceberg does not yet support deserializing avro blobs to geometry yet.
	if (type.id() == LogicalTypeId::GEOMETRY) {
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

bool IcebergMultiFileList::FileMatchesFilter(const IcebergManifestFile &manifest_file,
                                             const IcebergManifestEntry &manifest_entry,
                                             IcebergManifestContentType file_type) const {
	D_ASSERT(table_filters.HasFilters());
	auto &schema = GetSchema().columns;

	auto &metadata = GetMetadata();
	unordered_set<int32_t> mapping_field_ids;
	for (auto &mapping : metadata.mappings) {
		if (mapping.field_id != NumericLimits<int32_t>::Maximum()) {
			mapping_field_ids.insert(mapping.field_id);
		}
	}

	for (auto &entry : table_filters) {
		auto index = entry.first;
		auto &column = *schema[index];

		auto &data_file = manifest_entry.data_file;
		// First check if there are partitions
		if (!data_file.partition_info.empty()) {
			// check if the index is in the partition info.
			auto partition_spec_it = metadata.partition_specs.find(manifest_file.partition_spec_id);
			if (partition_spec_it == metadata.partition_specs.end()) {
				throw InvalidConfigurationException(
				    "Data file %s has partition spec %d while the metadata does not have this partition spec",
				    data_file.file_path, manifest_file.partition_spec_id);
			}
			auto &partition_spec = partition_spec_it->second;
			unordered_map<uint64_t, ColumnIndex> source_to_column_id;
			IcebergTableSchema::PopulateSourceIdMap(source_to_column_id, schema, nullptr);

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
				auto stats = IcebergPredicateStats();
				bool found_partition_field = false;
				for (auto &partition_val : data_file.partition_info) {
					if (field.partition_field_id == partition_val.field_id) {
						found_partition_field = true;
						stats.lower_bound = partition_val.value;
						stats.upper_bound = partition_val.value;
						stats.has_upper_bounds = true;
						stats.has_lower_bounds = true;
						// set null stats for partitioned column.
						if (partition_val.value.IsNull()) {
							// partition values can be null
							stats.has_null = true;
						} else {
							stats.has_not_null = true;
						}
						break;
					}
				}

				if (!found_partition_field) {
					// continue to next partition spec field summary
					continue;
				}

				auto nan_counts_it = data_file.nan_value_counts.find(column_id.GetPrimaryIndex());
				if (nan_counts_it != data_file.nan_value_counts.end()) {
					auto &nan_counts = nan_counts_it->second;
					stats.has_nan = nan_counts != 0;
				}

				// if the filter doesn't match the partition value, we don't need to scan the data file
				if (!IcebergPredicate::MatchBounds(context, *table_filter, stats, field.transform)) {
					auto &source_column = IcebergTableSchema::GetFromColumnIndex(schema, column_id, 0);
					auto partition_value_raw_str = stats.has_lower_bounds ? stats.lower_bound.ToString() : "NULL";
					auto partition_value_transformed_str =
					    stats.has_lower_bounds ? field.transform.PartitionValueToString(stats.lower_bound) : "NULL";
					DUCKDB_LOG(
					    context, IcebergLogType,
					    "Iceberg Filter Pushdown, skipped 'data_file': '%s', partition column '%s' has raw value %s "
					    "with transform '%s'. '%s(%s)=%s' does not match filter: %s",
					    data_file.file_path, source_column.name, partition_value_raw_str, field.transform.RawType(),
					    field.transform.RawType(), partition_value_raw_str, partition_value_transformed_str,
					    table_filter->ToString(source_column.name));
					return false;
				}
			}
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

		auto stats = IcebergPredicateStats::DeserializeBounds(lower_bound, upper_bound, column.name, column.type);

		int64_t value_count = 0;
		bool has_value_counts = false;
		auto value_counts_it = data_file.value_counts.find(column_id);
		if (value_counts_it != data_file.value_counts.end()) {
			value_count = value_counts_it->second;
			has_value_counts = true;
		}

		auto null_counts_it = data_file.null_value_counts.find(column_id);
		if (null_counts_it != data_file.null_value_counts.end()) {
			auto &null_counts = null_counts_it->second;
			stats.has_null = null_counts != 0;
			if (has_value_counts) {
				stats.has_not_null = (value_count - null_counts) > 0;
			} else {
				// if no value counts are active, assume there are values
				stats.has_not_null = true;
			}
		} else {
			stats.has_null = true;
			if (has_value_counts) {
				stats.has_not_null = value_count > 0;
			} else {
				// if no value counts are active, assume there are values
				stats.has_not_null = true;
			}
		}

		auto nan_counts_it = data_file.nan_value_counts.find(column_id);
		if (nan_counts_it != data_file.nan_value_counts.end()) {
			auto &nan_counts = nan_counts_it->second;
			stats.has_nan = nan_counts != 0;
		}

		auto &filter = *entry.second;
		if (!IcebergPredicate::MatchBounds(context, filter, stats, IcebergTransform::Identity())) {
			//! If any predicate fails, exclude the file
			DUCKDB_LOG(context, IcebergLogType,
			           "Iceberg Filter Pushdown, skipped 'data_file': '%s', column '%s' with "
			           "bounds [%s, %s] did not match filter: %s",
			           data_file.file_path, column.name, stats.has_lower_bounds ? stats.lower_bound.ToString() : "N/A",
			           stats.has_upper_bounds ? stats.upper_bound.ToString() : "N/A", filter.ToString(column.name));
			return false;
		}
	}
	return true;
}

optional_ptr<ManifestReadBatch> IcebergMultiFileList::TryGetNextBatch(lock_guard<mutex> &guard) const {
	auto batch = read_state.GetCurrentBatch();
	if (batch) {
		//! We are still reading a batch, or have batches to read
		return batch;
	}
	if (!data_manifest_read_state) {
		return batch;
	}
	auto &scheduler = TaskScheduler::GetScheduler(context);
	auto &scan_state = *data_manifest_read_state;
	auto &executor = scan_state.executor;
	shared_ptr<Task> task_to_execute;
	while (scan_state.in_progress_tasks) {
		if (executor.GetTask(task_to_execute)) {
			auto res = task_to_execute->Execute(TaskExecutionMode::PROCESS_PARTIAL);
			if (res == TaskExecutionResult::TASK_NOT_FINISHED) {
				auto &token = *task_to_execute->token;
				scheduler.ScheduleTask(token, std::move(task_to_execute));
			}
			batch = read_state.GetCurrentBatch();
			if (batch) {
				return batch;
			}
			//! We didn't manage to populate the buffer with our scan
			//! But another task might be in the process of scanning
			//! Have to wait for everything to finish to conclusively say we're done
		}
		executor.WorkOnTasks();
		return read_state.GetCurrentBatch();
	}
	return read_state.GetCurrentBatch();
}

void IcebergMultiFileList::FinishScanTasks(lock_guard<mutex> &guard) const {
	if (!data_manifest_read_state) {
		return;
	}
	auto &read_state = *data_manifest_read_state;
	auto &executor = read_state.executor;
	//! Make sure all tasks are done before shutting down
	executor.WorkOnTasks();
};

optional_ptr<const BoundIcebergManifestEntry> IcebergMultiFileList::GetDataFile(idx_t file_id,
                                                                                lock_guard<mutex> &guard) const {
	D_ASSERT(initialized);
	if (file_id < data_manifest_entries.size()) {
		//! Have we already scanned this data file and returned it? If so, return it
		return data_manifest_entries[file_id];
	}

	while (file_id >= data_manifest_entries.size()) {
		auto batch = TryGetNextBatch(guard);
		if (!batch) {
			FinishScanTasks(guard);
			return nullptr;
		}

		auto &current_batch = *batch;
		auto &bound_manifest_list_entry = data_manifests[current_batch.manifest_list_entry_idx];
		auto &manifest_list_entry = bound_manifest_list_entry.entry;
		auto &manifest_entries = manifest_list_entry.manifest_entries;
		auto &manifest_file = manifest_list_entry.file;
		for (; current_batch.start_index < current_batch.end_index && file_id >= data_manifest_entries.size();
		     current_batch.start_index++) {
			auto &manifest_entry = manifest_entries[current_batch.start_index];
			if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
				continue;
			}

			auto &data_file = manifest_entry.data_file;
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
		if (current_batch.start_index >= current_batch.end_index) {
			read_state.FinishBatch();
		}
	}
	return data_manifest_entries[file_id];
}

OpenFileInfo IcebergMultiFileList::GetFileInternal(idx_t file_id, lock_guard<mutex> &guard) const {
	if (!initialized) {
		InitializeFiles(guard);
	}

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
	lock_guard<mutex> guard(lock);
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
			           stats.has_lower_bounds ? stats.lower_bound.ToString() : "N/A",
			           stats.has_upper_bounds ? stats.upper_bound.ToString() : "N/A",
			           table_filter->ToString(column.name));
			return false;
		}
	}
	return true;
}

vector<reference<const IcebergEqualityDeleteRow>>
IcebergMultiFileList::GetEqualityDeletesForFile(const BoundIcebergManifestEntry &bound_manifest_entry) const {
	vector<reference<const IcebergEqualityDeleteRow>> result;

	//! Look through all the equality delete files with a *higher* sequence number
	auto &manifest_entry = bound_manifest_entry.entry;
	auto &manifest_file = data_manifests[bound_manifest_entry.manifest_file_idx].entry.file;
	auto &data_file = manifest_entry.data_file;
	auto &metadata = GetMetadata();
	auto it = equality_delete_data.upper_bound(manifest_entry.GetSequenceNumber(manifest_file));
	for (; it != equality_delete_data.end(); it++) {
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
			result.insert(result.end(), file.rows.begin(), file.rows.end());
		}
	}
	return result;
}

void IcebergMultiFileList::InitializeFiles(lock_guard<mutex> &guard) const {
	if (initialized) {
		return;
	}
	initialized = true;

	auto &snapshot_info = scan_info->snapshot_info;
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
			if (!ManifestMatchesFilter(manifest_file)) {
				//! Skip this manifest
				continue;
			}

			if (manifest_file.content == IcebergManifestContentType::DATA) {
				committed_data_manifests.push_back(std::move(manifest_list_entry));
			} else {
				D_ASSERT(manifest_file.content == IcebergManifestContentType::DELETE);
				committed_delete_manifests.push_back(std::move(manifest_list_entry));
			}
		}

		if (!committed_delete_manifests.empty()) {
			delete_manifest_scan = AvroScan::ScanManifest(snapshot_info, committed_delete_manifests, options, fs,
			                                              iceberg_path, metadata, context);
			delete_manifest_reader = make_uniq<manifest_file::ManifestReader>(*delete_manifest_scan);
		}
	}

	if (HasTransactionData()) {
		auto &transaction_data = GetTransactionData();
		for (auto &alter_p : transaction_data.alters) {
			auto &alter = alter_p.get();
			const auto &manifest_list_entries = alter.GetManifestFiles();
			for (auto &manifest_list_entry : manifest_list_entries) {
				auto &manifest = manifest_list_entry.file;
				if (!ManifestMatchesFilter(manifest)) {
					//! Skip this manifest
					continue;
				}
				switch (manifest.content) {
				case IcebergManifestContentType::DATA: {
					transaction_data_manifests.push_back(manifest_list_entry);
					break;
				}
				case IcebergManifestContentType::DELETE: {
					transaction_delete_manifests.push_back(manifest_list_entry);
					break;
				}
				default:
					throw NotImplementedException("IcebergManifestContentType: %d",
					                              static_cast<uint8_t>(manifest.content));
				}
			}
		}
	}

	idx_t total_data_manifests = 0;
	total_data_manifests += committed_data_manifests.size();
	total_data_manifests += transaction_data_manifests.size();
	data_manifests.reserve(total_data_manifests);

	//! Add all data manifests
	for (auto &manifest : committed_data_manifests) {
		auto manifest_list_entry_idx = data_manifests.size();
		// reserve upfront → guarantees no reallocation
		auto &file = manifest.file;
		idx_t reserve_size = file.existing_files_count + file.added_files_count + file.deleted_files_count;
		manifest.manifest_entries.reserve(reserve_size);

		data_manifests.emplace_back(manifest_list_entry_idx, manifest);
	}
	for (auto &manifest : transaction_data_manifests) {
		auto manifest_list_entry_idx = data_manifests.size();
		data_manifests.emplace_back(manifest_list_entry_idx, manifest);
		read_state.PushBatch(ManifestReadBatch {manifest_list_entry_idx, 0, manifest.get().manifest_entries.size()});
	}

	idx_t total_delete_manifests = 0;
	total_delete_manifests += committed_delete_manifests.size();
	total_delete_manifests += transaction_delete_manifests.size();
	delete_manifests.reserve(total_delete_manifests);

	//! Add all delete manifests
	for (auto &manifest : committed_delete_manifests) {
		auto index = delete_manifests.size();
		delete_manifests.emplace_back(index, manifest);
	}
	for (auto &manifest : transaction_delete_manifests) {
		auto index = delete_manifests.size();
		delete_manifests.emplace_back(index, manifest);
	}

	if (!committed_data_manifests.empty()) {
		auto &metadata = GetMetadata();
		auto &snapshot_info = GetSnapshot();
		auto iceberg_path = GetPath();
		auto &fs = FileSystem::GetFileSystem(context);

		auto data_scan = AvroScan::ScanManifest(snapshot_info, committed_data_manifests, options, fs, iceberg_path,
		                                        metadata, context, &read_state);
		data_manifest_read_state =
		    make_uniq<IcebergManifestScanningState>(context, std::move(data_scan), committed_data_manifests);
		data_manifest_reader = make_uniq<manifest_file::ManifestReader>(*data_manifest_read_state->scan);

		auto &executor = data_manifest_read_state->executor;
		auto &scheduler = TaskScheduler::GetScheduler(context);
		auto worker_thread_count = scheduler.NumberOfThreads();

		auto num_threads = MinValue<idx_t>(worker_thread_count, data_manifests.size());
		data_manifest_read_state->in_progress_tasks = num_threads;
		for (idx_t i = 0; i < num_threads; i++) {
			executor.ScheduleTask(make_uniq<ManifestReadTask>(*data_manifest_read_state));
		}
	}
}

void IcebergMultiFileList::EnumerateDeleteManifestEntries() const {
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
		delete_manifest_reader->Read();
	}

	if (!committed_delete_entries_enumerated) {
		for (idx_t i = 0; i < committed_delete_manifests.size(); i++) {
			auto &manifest = delete_manifests[i];
			auto &entries = manifest.entry.manifest_entries;
			auto &manifest_file = manifest.entry.file;
			for (auto &manifest_entry : entries) {
				if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
					continue;
				}
				auto &data_file = manifest_entry.data_file;
				if (table_filters.HasFilters() &&
				    !FileMatchesFilter(manifest_file, manifest_entry, IcebergManifestContentType::DELETE)) {
					DUCKDB_LOG(context, IcebergLogType, "Iceberg Filter Pushdown, skipped 'data_file': '%s'",
					           data_file.file_path);
					continue;
				}
				auto &referenced_data_file = data_file.referenced_data_file;
				if (!referenced_data_file.empty() && transactional_delete_files &&
				    transactional_delete_files->count(referenced_data_file)) {
					//! Skip this delete file, there's a transaction-local delete that makes it obsolete
					continue;
				}
				auto bound_entry = manifest.BindEntry(manifest_entry);
				delete_manifest_entries.push_back(std::move(bound_entry));
			}
		}
		committed_delete_entries_enumerated = true;
	}

	auto offset = committed_delete_manifests.size();
	while (transaction_delete_idx < transaction_delete_manifests.size()) {
		auto &delete_manifest = delete_manifests[offset + transaction_delete_idx];
		for (auto &manifest_entry : delete_manifest.entry.manifest_entries) {
			auto &data_file = manifest_entry.data_file;
			auto &referenced_data_file = data_file.referenced_data_file;
			if (!referenced_data_file.empty() && transactional_delete_files) {
				auto it = transactional_delete_files->find(referenced_data_file);
				//! Check if this is the currently active (last) delete file for this referenced_data_file
				if (it != transactional_delete_files->end() && it->second != data_file.file_path) {
					continue;
				}
			}
			//! FIXME: no file pruning for uncommitted data?
			auto bound_manifest_entry = delete_manifest.BindEntry(manifest_entry);
			delete_manifest_entries.push_back(std::move(bound_manifest_entry));
		}
		transaction_delete_idx++;
	}

	D_ASSERT(FinishedScanningDeletes());
}

void IcebergMultiFileList::ScanDeleteFiles(const vector<MultiFileColumnDefinition> &global_columns,
                                           const vector<ColumnIndex> &global_column_ids,
                                           const vector<idx_t> &projection_ids) const {
	for (auto &bound_manifest_entry : delete_manifest_entries) {
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
	scanned_delete_manifests = true;
}

void IcebergMultiFileList::ProcessDeletes(const vector<MultiFileColumnDefinition> &global_columns,
                                          const vector<ColumnIndex> &global_column_ids,
                                          const vector<idx_t> &projection_ids) const {
	//! Enumerate the delete manifest entries, then read the delete files they reference.
	//! EnumerateDeleteManifestEntries() is idempotent, so this is safe even if the entries were
	//! already enumerated earlier (e.g. by the optimizer).
	EnumerateDeleteManifestEntries();
	if (!scanned_delete_manifests) {
		ScanDeleteFiles(global_columns, global_column_ids, projection_ids);
	}
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
	vector<string> input_names;

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
			ScanEqualityDeleteFile(bound_manifest_entry, result, multi_file_local_state.reader->columns, global_columns,
			                       global_column_ids, projection_ids);
		} while (result.size() != 0);
	}
}

unique_ptr<DeleteFilter> IcebergMultiFileList::GetPositionalDeletesForFile(const string &file_path) const {
	auto it = positional_delete_data.find(file_path);
	if (it != positional_delete_data.end()) {
		// There is delete data for this file, return it
		return it->second->ToFilter();
	}
	return nullptr;
}

} // namespace duckdb
