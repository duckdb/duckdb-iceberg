#include "planning/scan_plan/iceberg_scan_planner.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/partition/iceberg_partition_constants.hpp"
#include "planning/pruning/iceberg_file_pruner.hpp"
#include "planning/scan_plan/iceberg_scan_plan_provider.hpp"
#include "planning/scan_plan/iceberg_scan_statistics.hpp"
#include "duckdb/storage/table/row_group_reorderer.hpp"
#include "duckdb/logging/logger.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {

IcebergScanPlanner::IcebergScanPlanner(ClientContext &context_p, shared_ptr<IcebergScanInfo> scan_info,
                                       const string &path, const IcebergOptions &options_p)
    : shared_state(make_shared_ptr<IcebergScanPlanState>(context_p, std::move(scan_info), path, options_p)),
      context(shared_state->context), fs(shared_state->fs) {
}

IcebergScanPlanner::IcebergScanPlanner(shared_ptr<IcebergScanPlanState> shared_state_p)
    : shared_state(std::move(shared_state_p)), context(shared_state->context), fs(shared_state->fs) {
}

IcebergScanPlanner::~IcebergScanPlanner() {
	if (scan_tasks) {
		DUCKDB_LOG(context, IcebergLogType,
		           "Iceberg metadata phase=delete_planning scope=planner_view scan_tasks=%llu "
		           "delete_candidate_visits=%llu delete_predicate_evaluations=%llu",
		           scan_tasks, delete_candidate_visits.load(), delete_predicate_evaluations.load());
	}
}

unique_ptr<IcebergScanPlanner> IcebergScanPlanner::CreateView(IcebergTableFilters filters) const {
	unique_ptr<RowGroupOrderOptions> filtered_scan_order;
	{
		annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
		shared_state->FreezeConfiguration();
		filtered_scan_order = scan_order.CopyOptions();
	}
	auto result = unique_ptr<IcebergScanPlanner>(new IcebergScanPlanner(shared_state));
	result->table_filters = std::move(filters);
	if (filtered_scan_order) {
		result->SetScanOrder(std::move(filtered_scan_order));
	}
	return result;
}

const string &IcebergScanPlanner::GetPath() const {
	return shared_state->Configuration().path;
}

const IcebergOptions &IcebergScanPlanner::GetOptions() const {
	return shared_state->Configuration().options;
}

const IcebergTableMetadata &IcebergScanPlanner::GetMetadata() const {
	return shared_state->Configuration().scan_info->metadata;
}

const IcebergTableSchema &IcebergScanPlanner::GetSchema() const {
	return shared_state->Configuration().scan_info->schema;
}

ClientContext &IcebergScanPlanner::GetContext() const {
	return context;
}

bool IcebergScanPlanner::HasScanInfo() const {
	return shared_state->Configuration().scan_info != nullptr;
}

bool IcebergScanPlanner::HasTransactionData() const {
	return shared_state->Configuration().scan_info->transaction_data;
}

const IcebergSnapshotScanInfo &IcebergScanPlanner::GetSnapshot() const {
	return shared_state->Configuration().scan_info->snapshot_info;
}

optional_ptr<IcebergTableSchemaVersion> IcebergScanPlanner::GetTable() const {
	return shared_state->Configuration().table;
}

void IcebergScanPlanner::SetTable(IcebergTableSchemaVersion &table) {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	shared_state->SetTable(table);
}

void IcebergScanPlanner::SetScanInfo(shared_ptr<IcebergScanInfo> scan_info) {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	shared_state->SetScanInfo(std::move(scan_info));
}

void IcebergScanPlanner::SetOptions(const IcebergOptions &new_options) {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	shared_state->SetOptions(new_options);
}

void IcebergScanPlanner::SetScanOrder(unique_ptr<RowGroupOrderOptions> order_options) {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	scan_order.Set(std::move(order_options));
}

void IcebergScanPlanner::DisableServerSidePlanning() {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	shared_state->DisableServerSidePlanning();
}

void IcebergScanPlanner::RequireRowIds() {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	shared_state->RequireRowIds();
}

const IcebergTableFilters &IcebergScanPlanner::Filters() const {
	return table_filters;
}

IcebergScanPlanProvider &IcebergScanPlanner::GetScanPlanProvider() const {
	return shared_state->GetScanPlanProvider(table_filters, scan_order);
}

IcebergDeletePlanningContext IcebergScanPlanner::GetDeletePlanningContext(IcebergScanPlanProvider &provider) const {
	return {context,          fs,
	        GetPath(),        GetOptions(),
	        GetMetadata(),    GetSchema(),
	        table_filters,    data_manifests,
	        delete_manifests, delete_manifest_matches,
	        provider};
}

void IcebergScanPlanner::InitializeView(annotated_lock_guard<annotated_mutex> &guard) const {
	if (view_initialized) {
		return;
	}
	GetScanPlanProvider().LoadManifestList();
	IcebergFilePruner pruner(context, GetMetadata(), GetSchema(), table_filters);
	auto &committed_data = GetScanPlanProvider().DataManifests();
	for (auto &manifest : committed_data) {
		data_manifests.emplace_back(data_manifests.size(), manifest);
		data_manifest_matches.push_back(pruner.ManifestMatchesFilter(manifest));
	}
	for (auto &manifest : GetScanPlanProvider().TransactionDataManifests()) {
		auto &metadata = manifest.get().manifest_metadata;
		D_ASSERT(metadata);
		auto lineage_mode =
		    metadata->format_version >= 3 ? IcebergRowLineageMode::STORED_ONLY : IcebergRowLineageMode::NONE;
		data_manifests.emplace_back(data_manifests.size(), manifest, lineage_mode);
		data_manifest_matches.push_back(pruner.ManifestMatchesFilter(manifest.get()));
	}
	auto &committed_deletes = GetScanPlanProvider().DeleteManifests();
	bool has_matching_deletes = false;
	for (auto &manifest : committed_deletes) {
		delete_manifests.emplace_back(delete_manifests.size(), manifest);
		auto matches = pruner.ManifestMatchesFilter(manifest);
		delete_manifest_matches.push_back(matches);
		has_matching_deletes |= matches;
	}
	for (auto &manifest : GetScanPlanProvider().TransactionDeleteManifests()) {
		delete_manifests.emplace_back(delete_manifests.size(), manifest);
		auto matches = pruner.ManifestMatchesFilter(manifest.get());
		delete_manifest_matches.push_back(matches);
		has_matching_deletes |= matches;
	}
	has_matching_delete_manifests.store(has_matching_deletes);
	view_initialized = true;
}

void IcebergScanPlanner::StartDataManifestScan(annotated_lock_guard<annotated_mutex> &) const {
	D_ASSERT(view_initialized);
	GetScanPlanProvider().StartDataManifestScan(data_manifest_matches, table_filters.FilterCount());
}

bool IcebergScanPlanner::TryGetNextBatch(annotated_lock_guard<annotated_mutex> &) const {
	return GetScanPlanProvider().TryGetNextBatch(data_view_cursor);
}

void IcebergScanPlanner::FinishScanTasks(annotated_lock_guard<annotated_mutex> &) const {
	GetScanPlanProvider().FinishScanTasks();
}

optional_ptr<const BoundIcebergManifestEntry>
IcebergScanPlanner::GetDataFile(idx_t file_id, annotated_lock_guard<annotated_mutex> &guard) const {
	InitializeView(guard);
	StartDataManifestScan(guard);
	auto &transaction_data = shared_state->Configuration().scan_info->transaction_data;
	if (file_id < data_manifest_entries.size()) {
		return data_manifest_entries[file_id];
	}
	while (file_id >= data_manifest_entries.size()) {
		if (!TryGetNextBatch(guard)) {
			FinishScanTasks(guard);
			return nullptr;
		}
		auto &batch = data_view_cursor.current_batch;
		auto &bound_manifest = data_manifests[batch.manifest_list_entry_idx];
		auto &manifest_entries = bound_manifest.entry.GetManifestEntries();
		auto &manifest_file = bound_manifest.entry.GetManifest();
		if (!data_manifest_matches[batch.manifest_list_entry_idx]) {
			data_view_cursor.current_batch_offset = batch.end_index;
		}
		for (; data_view_cursor.current_batch_offset < batch.end_index && file_id >= data_manifest_entries.size();
		     data_view_cursor.current_batch_offset++) {
			auto &manifest_entry = manifest_entries[data_view_cursor.current_batch_offset];
			auto &data_file = manifest_entry.data_file;
			auto entry_path = data_file.file_path;
			if (GetOptions().allow_moved_paths) {
				entry_path = IcebergUtils::GetFullPath(GetPath(), entry_path, fs);
			}
			IcebergPartition partition {manifest_file.partition_spec_id, data_file.partition_info};
			shared_state->data_file_partitions[entry_path] = partition;
			shared_state->data_file_partitions[data_file.file_path] = std::move(partition);
			auto bound_entry = bound_manifest.BindEntry(manifest_entry);
			if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
				continue;
			}
			//! Replaced by an earlier statement of this transaction, e.g. a rewrite of the data files
			if (transaction_data &&
			    transaction_data->IsFileInvalidated({data_file.file_path, data_file.content_offset})) {
				continue;
			}
			if (table_filters.HasFilters() && !IcebergFilePruner(context, GetMetadata(), GetSchema(), table_filters)
			                                       .FileMatchesFilter(manifest_file, manifest_entry)) {
				continue;
			}
			if (StringUtil::CIEquals(data_file.file_format, "puffin")) {
				continue;
			}
			data_manifest_entries.push_back(bound_entry);
		}
		if (data_view_cursor.current_batch_offset >= batch.end_index) {
			data_view_cursor.has_current_batch = false;
		}
	}
	return data_manifest_entries[file_id];
}

void IcebergScanPlanner::EnsureScanOrderApplied(annotated_lock_guard<annotated_mutex> &guard) const {
	if (!scan_order.IsPending()) {
		return;
	}
	idx_t materialized = 0;
	while (GetDataFile(materialized, guard)) {
		materialized++;
	}
	scan_order.Apply(context, GetSchema(), has_matching_delete_manifests.load(), data_manifest_entries);
}

IcebergDataFileDescriptor IcebergScanPlanner::CreateDataFileDescriptor(const BoundIcebergManifestEntry &entry) const {
	auto &file = entry.entry.data_file;
	auto &bound_manifest = data_manifests[entry.manifest_file_idx];
	auto &manifest = bound_manifest.entry.GetManifest();
	IcebergDataFileDescriptor task;
	task.original_file_path = file.file_path;
	task.file_path =
	    GetOptions().allow_moved_paths ? IcebergUtils::GetFullPath(GetPath(), file.file_path, fs) : file.file_path;
	task.file_format = file.file_format;
	task.file_size_in_bytes = file.file_size_in_bytes;
	task.record_count = file.record_count;
	task.sequence_number = entry.entry.GetSequenceNumber(manifest);
	task.first_row_id = entry.HasFirstRowId() ? optional<int64_t>(entry.GetFirstRowId()) : nullopt;
	task.row_lineage_mode = bound_manifest.row_lineage_mode;
	task.partition_spec_id = manifest.partition_spec_id;
	return task;
}

optional<IcebergDataFileDescriptor> IcebergScanPlanner::GetDataFileDescriptor(idx_t file_id) const {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	GetDataFile(file_id, guard);
	EnsureScanOrderApplied(guard);
	if (file_id >= data_manifest_entries.size()) {
		return nullopt;
	}
	return CreateDataFileDescriptor(data_manifest_entries[file_id]);
}

optional<IcebergFileScanTask> IcebergScanPlanner::GetScanTask(idx_t file_id) const {
	optional<BoundIcebergManifestEntry> entry;
	IcebergFileScanTask task;
	{
		annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
		GetDataFile(file_id, guard);
		EnsureScanOrderApplied(guard);
		if (file_id >= data_manifest_entries.size()) {
			return nullopt;
		}
		entry.emplace(data_manifest_entries[file_id]);
		task = IcebergFileScanTask(CreateDataFileDescriptor(*entry));
		task.partition_constants = IcebergPartitionConstants::Resolve(
		    task.partition_spec_id, entry->entry.data_file.partition_info, GetMetadata(), GetSchema());
		scan_tasks++;
	}
	// Delete manifest I/O must run without the shared planning lock.
	task.delete_files = ResolveApplicableDeleteFiles(*entry);
	return task;
}

idx_t IcebergScanPlanner::GetTotalFileCount() const {
	idx_t file_id = 0;
	while (GetDataFileDescriptor(file_id)) {
		file_id++;
	}
	return file_id;
}

bool IcebergScanPlanner::HasInvalidatedFiles() const {
	auto &transaction_data = shared_state->Configuration().scan_info->transaction_data;
	return transaction_data && transaction_data->HasInvalidatedFiles();
}

unique_ptr<NodeStatistics> IcebergScanPlanner::GetCardinality() const {
	//! The manifest counts still include the files replaced earlier in this transaction
	if (GetMetadata().iceberg_version == 1 || HasInvalidatedFiles()) {
		return nullptr;
	}
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	InitializeView(guard);
	auto cardinality =
	    IcebergScanStatistics(data_manifests, data_manifest_matches, delete_manifests, delete_manifest_matches)
	        .EstimateCardinality();
	if (!cardinality) {
		return nullptr;
	}
	return make_uniq<NodeStatistics>(*cardinality, *cardinality);
}

void IcebergScanPlanner::GetStatistics(vector<PartitionStatistics> &result) const {
	if (GetMetadata().iceberg_version == 1 || HasInvalidatedFiles()) {
		return;
	}
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	auto &transaction_data = shared_state->Configuration().scan_info->transaction_data;
	// Manifest counts still include files invalidated by uncommitted rewrites.
	if (transaction_data && !transaction_data->manifest_deletes.IsEmpty()) {
		return;
	}
	InitializeView(guard);
	auto count = IcebergScanStatistics(data_manifests, data_manifest_matches, delete_manifests, delete_manifest_matches)
	                 .ExactRowCount();
	if (!count) {
		return;
	}
	PartitionStatistics stats;
	stats.count = *count;
	stats.count_type = CountType::COUNT_EXACT;
	result.push_back(stats);
}

IcebergPartition IcebergScanPlanner::GetPartitionForDataFile(const string &file_path) const {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	auto entry = shared_state->data_file_partitions.find(file_path);
	if (entry != shared_state->data_file_partitions.end()) {
		return entry->second;
	}
	throw InvalidConfigurationException("Could not find data file '%s' in manifest entries", file_path);
}

vector<IcebergDeleteFile>
IcebergScanPlanner::ResolveApplicableDeleteFiles(const BoundIcebergManifestEntry &data_manifest_entry) const {
	vector<IcebergDeleteFile> result;
	if (!has_matching_delete_manifests.load()) {
		return result;
	}
	optional_ptr<IcebergScanPlanProvider> provider;
	{
		annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
		InitializeView(guard);
		provider = &GetScanPlanProvider();
	}
	// Descriptors and view filters are frozen. Only cursor/data-file publication
	// and acquisition of completed index handles use the broad planning lock.
	auto delete_context = GetDeletePlanningContext(*provider);
	auto manifest_indexes = IcebergDeletePlanner::GetDeleteManifestsForDataFile(delete_context, data_manifest_entry);
	if (manifest_indexes.empty()) {
		return result;
	}
	provider->ReadDeleteManifests(manifest_indexes, table_filters.FilterCount());
	vector<IcebergDeleteManifestHandle> indexes;
	{
		annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
		indexes = provider->GetDeleteIndexes(manifest_indexes);
	}
	auto &data_file = data_manifest_entry.entry.data_file;
	auto partition_values = IcebergFilePruner::PartitionValueMap(data_file);
	auto spec_id = data_manifests[data_manifest_entry.manifest_file_idx].entry.GetManifest().partition_spec_id;
	idx_t candidates_visited = 0;
	for (auto &handle : indexes) {
		auto buckets = handle.index->Lookup(data_file.file_path, spec_id, partition_values);
		for (auto &bucket : buckets) {
			candidates_visited += bucket.get().size();
			for (auto entry_idx : bucket.get()) {
				auto &delete_entry = handle.index->Entries()[entry_idx];
				if (!IcebergDeletePlanner::DeleteEntryMatchesDataFileReference(delete_context, delete_entry,
				                                                               data_manifest_entry)) {
					continue;
				}
				if (table_filters.HasFilters() &&
				    !DeleteEntryMatchesFilters(delete_context, handle.manifest_idx, entry_idx, delete_entry)) {
					continue;
				}
				optional_ptr<IcebergEqualityDeleteBounds> bounds;
				if (delete_entry.data_file.content == IcebergManifestEntryContentType::EQUALITY_DELETES) {
					bounds = &shared_state->GetEqualityDeleteBounds(handle.manifest_idx, entry_idx);
				}
				if (IcebergDeletePlanner::DeleteEntryAppliesToDataFile(delete_context, handle.manifest_idx,
				                                                       delete_entry, data_manifest_entry,
				                                                       partition_values, bounds)) {
					result.emplace_back(delete_entry.data_file);
				}
			}
		}
	}
	delete_candidate_visits += candidates_visited;
	return result;
}

bool IcebergScanPlanner::DeleteEntryMatchesFilters(const IcebergDeletePlanningContext &delete_context,
                                                   idx_t manifest_idx, idx_t entry_idx,
                                                   const IcebergManifestEntry &entry) const {
	optional_ptr<DeletePredicateResult> cached;
	{
		lock_guard<mutex> guard(delete_predicate_lock);
		auto &slot = delete_entry_matches[manifest_idx][entry_idx];
		if (!slot) {
			slot = make_uniq<DeletePredicateResult>();
		}
		cached = slot.get();
	}
	// The stable entry allocation survives map growth. No cache lock is held
	// while acquiring the planning lock (matching never reacquires that lock).
	lock_guard<mutex> guard(cached->lock);
	if (!cached->matches) {
		auto matches = IcebergDeletePlanner::DeleteEntryMatchesFilters(delete_context, manifest_idx, entry);
		cached->matches = matches;
		delete_predicate_evaluations++;
	}
	return *cached->matches;
}

} // namespace duckdb
