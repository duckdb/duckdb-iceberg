#include "planning/iceberg_multi_file_list.hpp"

#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/table/row_group_reorderer.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "planning/deletes/iceberg_delete_file_scanner.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "planning/pruning/iceberg_file_pruner.hpp"
#include "planning/scan_plan/iceberg_scan_plan_provider.hpp"

namespace duckdb {

namespace {

static void MergeDeleteScanResult(IcebergScanPlanProvider &provider, IcebergDeleteScanResult &&scan_result) {
	auto &positional_delete_data = provider.PositionalDeleteData();
	for (auto &entry : scan_result.positional_delete_data) {
		auto existing = positional_delete_data.find(entry.first);
		if (existing == positional_delete_data.end()) {
			positional_delete_data.emplace(entry.first, std::move(entry.second));
			continue;
		}

		auto &target = existing->second;
		auto &source = entry.second;
		if (target->type == IcebergDeleteType::DELETION_VECTOR) {
			if (source->type == IcebergDeleteType::DELETION_VECTOR) {
				throw InvalidConfigurationException(
				    "Table is corrupt, two or more deletion vectors exist for the same referenced_data_file");
			}
			continue;
		}
		if (source->type == IcebergDeleteType::DELETION_VECTOR) {
			target = std::move(source);
			continue;
		}

		auto &target_positions = static_cast<IcebergPositionalDeleteData &>(*target);
		auto &source_positions = static_cast<IcebergPositionalDeleteData &>(*source);
		for (auto &source_entry : source_positions.entries) {
			target_positions.entries.push_back(source_entry);
		}
		target_positions.invalid_rows.insert(source_positions.invalid_rows.begin(),
		                                     source_positions.invalid_rows.end());
	}

	for (auto &entry : scan_result.equality_delete_data) {
		if (entry.delete_file->equality_values.size() == 0) {
			continue;
		}
		lock_guard<mutex> guard(entry.load->lock);
		entry.load->equality_delete = std::move(entry.delete_file);
	}
}

static void CompleteDeleteFileLoads(const vector<shared_ptr<IcebergDeleteFileLoadState>> &loads,
                                    const ErrorData &error) {
	for (auto &load : loads) {
		{
			lock_guard<mutex> guard(load->lock);
			load->error = error;
			load->complete = true;
		}
		load->cv.notify_all();
	}
}

} // namespace

IcebergMultiFileList::IcebergMultiFileList(ClientContext &context_p, shared_ptr<IcebergScanInfo> scan_info,
                                           const string &path, const IcebergOptions &options)
    : shared_state(make_shared_ptr<IcebergScanPlanState>(context_p, std::move(scan_info), path, options)),
      context(shared_state->context), fs(shared_state->fs), options(shared_state->options) {
}

IcebergMultiFileList::IcebergMultiFileList(shared_ptr<IcebergScanPlanState> shared_state_p)
    : shared_state(std::move(shared_state_p)), context(shared_state->context), fs(shared_state->fs),
      options(shared_state->options) {
}

IcebergMultiFileList::~IcebergMultiFileList() {
}

const string &IcebergMultiFileList::GetPath() const {
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

IcebergScanPlanProvider &IcebergMultiFileList::GetScanPlanProvider() const {
	D_ASSERT(scan_plan_provider);
	return *scan_plan_provider;
}

IcebergScanPlanContext IcebergMultiFileList::GetScanPlanContext() const {
	optional_ptr<const IcebergTransactionData> transaction_data;
	if (HasTransactionData()) {
		transaction_data = &GetTransactionData();
	}
	return {context, fs, GetPath(), options, GetSnapshot(), GetMetadata(), GetSchema(), transaction_data};
}

IcebergDeletePlanningContext IcebergMultiFileList::GetDeletePlanningContext() const {
	return {context,
	        fs,
	        GetPath(),
	        options,
	        GetMetadata(),
	        GetSchema(),
	        table_filters,
	        data_manifests,
	        delete_manifests,
	        delete_manifest_matches,
	        GetScanPlanProvider()};
}

optional_ptr<IcebergTableSchemaVersion> IcebergMultiFileList::GetTable() const {
	return shared_state->table;
}

void IcebergMultiFileList::SetTable(IcebergTableSchemaVersion &table) {
	shared_state->table = table;
}

void IcebergMultiFileList::SetOptions(const IcebergOptions &options) {
	shared_state->options = options;
}

void IcebergMultiFileList::SetScanOrder(unique_ptr<RowGroupOrderOptions> options) {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	scan_order.Set(std::move(options));
}

void IcebergMultiFileList::DisableServerSidePlanning() {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	if (!shared_state->manifest_list_loaded) {
		shared_state->server_side_planning_enabled = false;
	}
}

void IcebergMultiFileList::Bind(vector<LogicalType> &return_types, vector<Identifier> &names) {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);

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

unique_ptr<IcebergMultiFileList>
IcebergMultiFileList::PushdownInternal(ClientContext &context, TableFilterSet &new_filters,
                                       const vector<ColumnIndex> &column_indexes) const {
	unique_ptr<RowGroupOrderOptions> filtered_scan_order;
	{
		annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
		filtered_scan_order = scan_order.CopyOptions();
	}
	auto filtered_list = unique_ptr<IcebergMultiFileList>(new IcebergMultiFileList(shared_state));

	IcebergTableFilters result_filter_set;

	// The supplied filter set is the complete set of filters for the new view.
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

	filtered_list->table_filters = std::move(result_filter_set);
	filtered_list->names = names;
	filtered_list->types = types;
	filtered_list->have_bound = true;
	if (filtered_scan_order) {
		filtered_list->SetScanOrder(std::move(filtered_scan_order));
	}
	return filtered_list;
}

unique_ptr<MultiFileList>
IcebergMultiFileList::DynamicFilterPushdown(MultiFileDynamicPushdownInfo &pushdown_info) const {
	auto &column_indexes = pushdown_info.column_indexes;
	auto &context = pushdown_info.context;
	auto &filters = pushdown_info.filters;

	if (!filters.HasFilters()) {
		return nullptr;
	}

	auto filters_copy = filters.Copy();
	D_ASSERT(filters_copy->FilterCount() >= table_filters.FilterCount());
	bool filters_changed = false;
	for (auto &entry : filters) {
		auto &filter =
		    ExpressionFilter::GetExpressionFilter(entry.Filter(), "IcebergMultiFileList::DynamicFilterPushdown");
		auto column_id = column_indexes[entry.GetIndex().GetIndex()];
		auto previously_pushed_down_filter = table_filters.TryGetFilterByColumnIndex(column_id);
		if (!previously_pushed_down_filter || !filter.Equals(*previously_pushed_down_filter)) {
			filters_changed = true;
		}
	}

	if (filters_changed) {
		// Dynamic filter pushdown supplies the complete effective filter for every column. This includes filters
		// already pushed down by ComplexFilterPushdown, potentially combined with a new runtime filter.
		auto new_snap = PushdownInternal(context, *filters_copy, column_indexes);
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

	FilterCombiner combiner(context);
	for (const auto &filter : filters) {
		combiner.AddFilter(filter->Copy());
	}

	vector<FilterPushdownResult> unused;
	auto filter_set = combiner.GenerateTableScanFilters(info.column_indexes, unused);
	if (!filter_set.HasFilters()) {
		return nullptr;
	}

	return PushdownInternal(context, filter_set, info.column_indexes);
}

vector<OpenFileInfo> IcebergMultiFileList::GetAllFiles() const {
	vector<OpenFileInfo> file_list;
	//! Lock is required because it reads the 'manifest_entries' vector
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	for (idx_t i = 0;; i++) {
		auto file = GetFileInternal(i, guard);
		if (file.path.empty()) {
			break;
		}
		file_list.push_back(std::move(file));
	}
	return file_list;
}

FileExpandResult IcebergMultiFileList::GetExpandResult() const {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	// DuckDB calls MultiFileList::IsEmpty() during MultiFileBindInternal, before the reader's Bind() has
	// applied named parameters (SetOptions) or resolved metadata for a path-based scan.
	// do not call GetFileInternal before binding, otherwise we loose arguemnts from ICEBERG_SCAN like 'version' or
	// 'snapshot_from_id'
	if (have_bound) {
		// GetFileInternal(1) will ensure files with index 0 and index 1 are expanded if they are available
		GetFileInternal(1, guard);
	}

	// always return multiple files, In the case there is only 1 data file,
	// we only lose performance if it is small
	return FileExpandResult::MULTIPLE_FILES;
}

idx_t IcebergMultiFileList::GetTotalFileCount() const {
	// FIXME: the 'added_files_count' + the 'existing_files_count'
	// in the Manifest List should give us this information without scanning the manifest file(s)
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);

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

	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	InitializeView(guard);

	idx_t cardinality = 0;
	for (idx_t i = 0; i < data_manifests.size(); i++) {
		auto &manifest = data_manifests[i].entry.file;
		if (!data_manifest_matches[i]) {
			continue;
		}
		if (!manifest.counts || !manifest.counts->added_rows_count || !manifest.counts->existing_rows_count) {
			return nullptr;
		}
		cardinality += *manifest.counts->added_rows_count;
		cardinality += *manifest.counts->existing_rows_count;
	}
	for (idx_t i = 0; i < delete_manifests.size(); i++) {
		auto &manifest = delete_manifests[i].entry.file;
		if (!delete_manifest_matches[i]) {
			continue;
		}
		if (!manifest.counts || !manifest.counts->added_rows_count) {
			return nullptr;
		}
		cardinality -= *manifest.counts->added_rows_count;
	}
	return make_uniq<NodeStatistics>(cardinality, cardinality);
}

BoundIcebergManifestEntry IcebergMultiFileList::GetManifestEntry(idx_t file_id) const {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	return data_manifest_entries[file_id];
}

IcebergManifestFile IcebergMultiFileList::GetManifestFileForDataFile(idx_t file_id) const {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	auto manifest_file_idx = data_manifest_entries[file_id].manifest_file_idx;
	return data_manifests[manifest_file_idx].entry.file;
}

IcebergPartition IcebergMultiFileList::GetPartitionForDataFile(const string &file_path) const {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	auto entry = shared_state->data_file_partitions.find(file_path);
	if (entry != shared_state->data_file_partitions.end()) {
		return entry->second;
	}
	throw InvalidConfigurationException("Could not find data file '%s' in manifest entries", file_path);
}

const IcebergManifestFile &IcebergMultiFileList::GetManifestFileForEntry(const BoundIcebergManifestEntry &entry,
                                                                         IcebergManifestContentType type) const {
	if (type == IcebergManifestContentType::DATA) {
		return data_manifests[entry.manifest_file_idx].entry.file;
	} else {
		return delete_manifests[entry.manifest_file_idx].entry.file;
	}
}

IcebergPartitionRowGroup::IcebergPartitionRowGroup(const vector<unique_ptr<IcebergColumnDefinition>> &schema_p,
                                                   vector<reference<const IcebergDataFile>> data_files_p)
    : schema(schema_p), data_files(std::move(data_files_p)) {
}

bool IcebergPartitionRowGroup::SupportsExactMinMaxBounds(const LogicalType &type) {
	//! FLOAT/DOUBLE are excluded because of the Iceberg spec (not a DuckDB limit):
	//! the spec defines float/double manifest lower_bounds/upper_bounds to *exclude*
	//! NaN (tracked separately via nan_value_counts) and to treat -0.0 == +0.0.
	//! SQL MIN/MAX in DuckDB orders NaN as the largest value, so a file containing
	//! NaN would have an upper_bound that disagrees with the true MAX. The manifest
	//! bounds are therefore not a safe source of truth for exact float/double
	//! MIN/MAX, so we fall back to scanning for those types.
	//! See https://iceberg.apache.org/spec/#appendix-d-single-value-serialization
	if (type.id() == LogicalTypeId::FLOAT || type.id() == LogicalTypeId::DOUBLE) {
		return false;
	}
	//! VARCHAR/BLOB are implicitly excluded here as well: the spec permits writers
	//! to store *truncated* string/binary bounds (lower rounded down, upper rounded
	//! up), which bound the true value but are not exact enough to fold MIN/MAX.
	return type.IsNumeric() || type.IsTemporal();
}

unique_ptr<BaseStatistics> IcebergPartitionRowGroup::GetColumnStatistics(const StorageIndex &storage_index) {
	//! Build exact table-level min/max for one column from Iceberg manifest bounds.
	//! Returning nullptr tells StatisticsPropagator::TryExecuteAggregates to fall
	//! back to a normal scan for MIN/MAX on this column.
	//!
	//! storage_index is the logical column position in the Iceberg schema (same
	//! mapping LogicalGet uses for path-based iceberg_scan). Nested paths are
	//! not supported — only top-level columns.

	if (storage_index.HasChildren()) {
		return nullptr;
	}
	auto col_idx = storage_index.GetPrimaryIndex();
	if (col_idx >= schema.size()) {
		return nullptr;
	}
	auto &column = *schema[col_idx];
	auto &type = column.type;
	//! Skip types whose written bounds are not exact (truncated strings, floats
	//! with NaN). See SupportsExactMinMaxBounds.
	if (!SupportsExactMinMaxBounds(type)) {
		return nullptr;
	}
	if (data_files.empty()) {
		return nullptr;
	}

	//! Iceberg keys bounds by field id (stable across renames), not by name/index.
	const auto field_id = column.id;
	//! Reduce per-file [lower, upper] into a single table-wide [min, max].
	//! Each file's lower_bound is the exact min of that file (for allowlisted
	//! types), so min(lower across files) is the exact table MIN — same for MAX.
	optional<Value> global_min;
	optional<Value> global_max;
	for (auto &file_ref : data_files) {
		auto &data_file = file_ref.get();
		auto lower_it = data_file.lower_bounds.find(field_id);
		auto upper_it = data_file.upper_bounds.find(field_id);
		//! Bounds are optional in the spec (e.g. all-null columns, metrics=none).
		//! If any file lacks them we cannot prove an exact global min/max.
		if (lower_it == data_file.lower_bounds.end() || upper_it == data_file.upper_bounds.end()) {
			return nullptr;
		}
		//! Manifest stores bounds as Iceberg binary blobs; decode to typed Values.
		IcebergPredicateStats pred_stats;
		try {
			pred_stats =
			    IcebergPredicateStats::DeserializeBounds(lower_it->second, upper_it->second, column.name, type);
		} catch (...) {
			//! Corrupt / unexpected encoding — be conservative and skip the fold.
			return nullptr;
		}
		//! Null decoded bounds (e.g. all-null file) are not usable for MIN/MAX.
		if (!pred_stats.lower_bound || !pred_stats.upper_bound || pred_stats.lower_bound->IsNull() ||
		    pred_stats.upper_bound->IsNull()) {
			return nullptr;
		}
		if (!global_min || *pred_stats.lower_bound < *global_min) {
			global_min = *pred_stats.lower_bound;
		}
		if (!global_max || *pred_stats.upper_bound > *global_max) {
			global_max = *pred_stats.upper_bound;
		}
	}
	D_ASSERT(global_min && global_max);

	//! Package as DuckDB NumericStats so TryExecuteAggregates / MinMaxIsExact
	//! can fold SELECT MIN/MAX into a constant without scanning data files.
	auto stats = NumericStats::CreateEmpty(type);
	NumericStats::SetMin(stats, *global_min);
	NumericStats::SetMax(stats, *global_max);
	return stats.ToUnique();
}

bool IcebergPartitionRowGroup::MinMaxIsExact(const BaseStatistics &stats, const StorageIndex &storage_index) {
	//! GetColumnStatistics only returns stats for exact-capable types when every
	//! data file has usable (non-truncated) lower and upper bounds, so those
	//! values are exact table min/max.
	(void)stats;
	(void)storage_index;
	return true;
}

void IcebergMultiFileList::GetStatistics(vector<PartitionStatistics> &result) const {
	if (GetMetadata().iceberg_version == 1) {
		//! We collect no statistics information from manifests for V1 tables.
		return;
	}
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	InitializeView(guard);

	lock_guard<mutex> guard(shared_state->lock);
	InitializeFiles(guard);

	for (idx_t i = 0; i < delete_manifests.size(); i++) {
		if (delete_manifest_matches[i]) {
			//! Matching delete manifests can invalidate data-file bounds / counts;
			//! omit partition stats so MIN/MAX/COUNT(*) fall back to a scan.
			return;
		}
	}

	idx_t count = 0;
	for (idx_t i = 0; i < data_manifests.size(); i++) {
		auto &manifest = data_manifests[i].entry.file;
		if (!data_manifest_matches[i]) {
			continue;
		}
		if (!manifest.counts || !manifest.counts->added_rows_count || !manifest.counts->existing_rows_count) {
			return;
		}
		count += *manifest.counts->existing_rows_count;
		count += *manifest.counts->added_rows_count;
	}

	//! Materialize data-file entries so column bounds are available for MIN/MAX.
	idx_t materialized = 0;
	while (GetDataFile(materialized, guard)) {
		materialized++;
	}
	vector<reference<const IcebergDataFile>> data_files;
	data_files.reserve(data_manifest_entries.size());
	for (auto &entry : data_manifest_entries) {
		data_files.push_back(entry.entry.data_file);
	}

	PartitionStatistics partition_stats;
	partition_stats.count = count;
	partition_stats.count_type = CountType::COUNT_EXACT;
	//! Manifest column bounds (not Iceberg partition-statistics files) — those
	//! files have no per-column min/max.
	partition_stats.partition_row_group =
	    make_shared_ptr<IcebergPartitionRowGroup>(GetSchema().columns, std::move(data_files));
	result.push_back(std::move(partition_stats));
}

bool IcebergMultiFileList::TryGetNextBatch(annotated_lock_guard<annotated_mutex> &guard) const {
	return GetScanPlanProvider().TryGetNextBatch(data_view_cursor);
}

void IcebergMultiFileList::FinishScanTasks(annotated_lock_guard<annotated_mutex> &guard) const {
	GetScanPlanProvider().FinishScanTasks();
};

optional_ptr<const BoundIcebergManifestEntry>
IcebergMultiFileList::GetDataFile(idx_t file_id, annotated_lock_guard<annotated_mutex> &guard) const {
	D_ASSERT(scan_plan_provider);
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
			IcebergPartition partition {manifest_file.partition_spec_id, data_file.partition_info};
			shared_state->data_file_partitions[entry_path] = partition;
			shared_state->data_file_partitions[data_file.file_path] = std::move(partition);

			//! Bind the entry in order for the manifest list entry to record its row count
			auto bound_entry = bound_manifest_list_entry.BindEntry(manifest_entry);
			if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
				continue;
			}

			// Check whether current data file is filtered out.
			if (table_filters.HasFilters() && !IcebergFilePruner(context, GetMetadata(), GetSchema(), table_filters)
			                                       .FileMatchesFilter(manifest_file, manifest_entry)) {
				// Note: FileMatches filter will log a message if the file is pruned
				//! Skip this file
				continue;
			}

			// Check whether current data file belongs to an unknown puffin file, skip if so.
			if (StringUtil::CIEquals(data_file.file_format, "puffin")) {
				//! Skip this file
				continue;
			}
			data_manifest_entries.push_back(bound_entry);
		}
		if (view_cursor.current_batch_offset >= current_batch.end_index) {
			view_cursor.has_current_batch = false;
		}
	}
	return data_manifest_entries[file_id];
}

void IcebergMultiFileList::EnsureScanOrderApplied(annotated_lock_guard<annotated_mutex> &guard) const {
	if (!scan_order.IsPending()) {
		return;
	}

	idx_t materialized = 0;
	while (GetDataFile(materialized, guard)) {
		materialized++;
	}
	scan_order.Apply(context, GetSchema(), has_matching_delete_manifests.load(), data_manifest_entries);
}

OpenFileInfo IcebergMultiFileList::GetFileInternal(idx_t file_id, annotated_lock_guard<annotated_mutex> &guard) const {
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
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	return GetFileInternal(file_id, guard);
}

void IcebergMultiFileList::InitializeView(annotated_lock_guard<annotated_mutex> &guard) const {
	if (scan_plan_provider) {
		return;
	}
	LoadManifestList(guard);

	auto &committed_data_manifests = GetScanPlanProvider().DataManifests();
	auto &transaction_data_manifests = shared_state->transaction_data_manifests;
	IcebergFilePruner pruner(context, GetMetadata(), GetSchema(), table_filters);
	data_manifests.reserve(committed_data_manifests.size() + transaction_data_manifests.size());
	data_manifest_matches.reserve(committed_data_manifests.size() + transaction_data_manifests.size());
	for (auto &manifest : committed_data_manifests) {
		data_manifests.emplace_back(data_manifests.size(), manifest);
		data_manifest_matches.push_back(pruner.ManifestMatchesFilter(manifest.file));
	}
	for (auto &manifest : transaction_data_manifests) {
		data_manifests.emplace_back(data_manifests.size(), manifest);
		data_manifest_matches.push_back(pruner.ManifestMatchesFilter(manifest.get().file));
	}

	auto &committed_delete_manifests = GetScanPlanProvider().DeleteManifests();
	auto &transaction_delete_manifests = shared_state->transaction_delete_manifests;
	delete_manifests.reserve(committed_delete_manifests.size() + transaction_delete_manifests.size());
	delete_manifest_matches.reserve(committed_delete_manifests.size() + transaction_delete_manifests.size());
	bool view_has_matching_delete_manifests = false;
	for (auto &manifest : committed_delete_manifests) {
		delete_manifests.emplace_back(delete_manifests.size(), manifest);
		auto matches = pruner.ManifestMatchesFilter(manifest.file);
		delete_manifest_matches.push_back(matches);
		view_has_matching_delete_manifests |= matches;
	}
	for (auto &manifest : transaction_delete_manifests) {
		delete_manifests.emplace_back(delete_manifests.size(), manifest);
		auto matches = pruner.ManifestMatchesFilter(manifest.get().file);
		delete_manifest_matches.push_back(matches);
		view_has_matching_delete_manifests |= matches;
	}
	has_matching_delete_manifests.store(view_has_matching_delete_manifests);
}

void IcebergMultiFileList::InitializeScanPlanProvider() const {
	if (scan_plan_provider) {
		return;
	}
	scan_plan_provider = IcebergScanPlanProvider::Create(*shared_state, GetScanPlanContext(), GetTable(), table_filters,
	                                                     scan_order, shared_state->server_side_planning_enabled);
}

void IcebergMultiFileList::LoadManifestList(annotated_lock_guard<annotated_mutex> &guard) const {
	InitializeScanPlanProvider();
	GetScanPlanProvider().LoadManifestList();
}

void IcebergMultiFileList::StartDataManifestScan(annotated_lock_guard<annotated_mutex> &guard) const {
	D_ASSERT(scan_plan_provider);
	GetScanPlanProvider().StartDataManifestScan(data_manifest_matches, table_filters.FilterCount());
}

vector<IcebergDeleteFileReference>
IcebergMultiFileList::ResolveApplicableDeleteFiles(const BoundIcebergManifestEntry &data_manifest_entry) const {
	vector<IcebergDeleteFileReference> result;
	if (!has_matching_delete_manifests.load()) {
		return result;
	}

	vector<idx_t> manifest_indexes;
	optional_ptr<IcebergScanPlanProvider> provider;
	{
		annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
		InitializeView(guard);
		manifest_indexes =
		    IcebergDeletePlanner::GetDeleteManifestsForDataFile(GetDeletePlanningContext(), data_manifest_entry);
		provider = scan_plan_provider.get();
	}
	if (manifest_indexes.empty()) {
		return result;
	}

	D_ASSERT(provider);
	provider->ReadDeleteManifests(manifest_indexes, table_filters.FilterCount());

	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	annotated_lock_guard<annotated_mutex> delete_guard(shared_state->delete_lock);
	auto delete_context = GetDeletePlanningContext();
	auto data_partition_values = IcebergFilePruner::PartitionValueMap(data_manifest_entry.entry.data_file);
	for (auto delete_file : provider->GetDeleteFiles(manifest_indexes)) {
		if (delete_file.manifest_idx >= delete_manifests.size()) {
			throw InternalException("Delete manifest index %llu is out of bounds for %llu manifests",
			                        delete_file.manifest_idx, delete_manifests.size());
		}
		auto &manifest_entries = delete_manifests[delete_file.manifest_idx].entry.GetManifestEntries();
		if (delete_file.entry_idx >= manifest_entries.size()) {
			throw InternalException("Delete manifest entry index %llu is out of bounds for manifest %llu",
			                        delete_file.entry_idx, delete_file.manifest_idx);
		}
		auto &delete_entry = manifest_entries[delete_file.entry_idx];
		if (!IcebergDeletePlanner::DeleteEntryMatchesFilters(delete_context, delete_file.manifest_idx, delete_entry)) {
			continue;
		}
		if (!IcebergDeletePlanner::DeleteEntryAppliesToDataFile(delete_context, delete_file.manifest_idx, delete_entry,
		                                                        data_manifest_entry, data_partition_values)) {
			continue;
		}
		result.push_back(delete_file);
	}
	return result;
}

IcebergDeletePlan IcebergMultiFileList::ProcessDeletes(const BoundIcebergManifestEntry &data_manifest_entry) const {
	IcebergDeletePlan result;
	if (!has_matching_delete_manifests.load()) {
		return result;
	}

	auto applicable_delete_files = ResolveApplicableDeleteFiles(data_manifest_entry);

	optional_ptr<IcebergScanPlanProvider> provider;
	vector<IcebergDeleteScanEntry> scan_entries;
	vector<shared_ptr<IcebergDeleteFileLoadState>> required_loads;
	vector<shared_ptr<IcebergDeleteFileLoadState>> new_loads;
	unique_ptr<IcebergDeletePlanningContext> delete_context;
	{
		annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
		annotated_lock_guard<annotated_mutex> delete_guard(shared_state->delete_lock);
		provider = scan_plan_provider.get();
		if (!provider) {
			//! ResolveApplicableDeleteFiles ran InitializeView, which creates the provider, so it is always
			//! set here - a null is a broken invariant, not a "nothing to do".
			throw InternalException("scan_plan_provider is not initialized in ProcessDeletes");
		}
		delete_context = make_uniq<IcebergDeletePlanningContext>(GetDeletePlanningContext());
		unordered_set<IcebergDeleteFileLoadState *> seen_loads;
		for (auto delete_file : applicable_delete_files) {
			auto &delete_manifest = delete_manifests[delete_file.manifest_idx].entry;
			auto &load = provider->GetDeleteFileLoad(delete_file);
			if (!load) {
				load = make_shared_ptr<IcebergDeleteFileLoadState>();
				new_loads.push_back(load);
				scan_entries.emplace_back(delete_file.manifest_idx, delete_file.entry_idx, delete_manifest, load);
			}
			if (seen_loads.insert(load.get()).second) {
				required_loads.push_back(load);
			}
		}
	}

	if (!scan_entries.empty()) {
		ErrorData scan_error;
		try {
			auto scan_result = IcebergDeleteFileScanner::ScanFiles(*delete_context, scan_entries);
			annotated_lock_guard<annotated_mutex> delete_guard(shared_state->delete_lock);
			MergeDeleteScanResult(*provider, std::move(scan_result));
		} catch (std::exception &ex) {
			scan_error = ErrorData(ex);
		} catch (...) { // LCOV_EXCL_START
			scan_error = ErrorData("Unknown exception while reading Iceberg delete files");
		} // LCOV_EXCL_STOP
		CompleteDeleteFileLoads(new_loads, scan_error);
	}

	for (auto &load : required_loads) {
		unique_lock<mutex> guard(load->lock);
		load->cv.wait(guard, [&load] { return load->complete; });
		if (load->error.HasError()) {
			load->error.Throw();
		}
		if (load->equality_delete) {
			result.equality_deletes.emplace_back(*load->equality_delete);
		}
	}

	{
		annotated_lock_guard<annotated_mutex> delete_guard(shared_state->delete_lock);
		auto &positional_delete_data = provider->PositionalDeleteData();
		auto entry = positional_delete_data.find(data_manifest_entry.entry.data_file.file_path);
		if (entry != positional_delete_data.end()) {
			result.positional_deletes = entry->second->ToFilter();
		}
	}
	return result;
}

shared_ptr<IcebergDeleteData> IcebergMultiFileList::GetExistingPositionalDeleteData(const string &file_path) const {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	annotated_lock_guard<annotated_mutex> delete_guard(shared_state->delete_lock);
	return IcebergDeletePlanner::GetExistingPositionalDeleteData(GetDeletePlanningContext(), file_path);
}

} // namespace duckdb
