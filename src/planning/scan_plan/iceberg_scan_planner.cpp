#include "planning/scan_plan/iceberg_scan_planner.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "planning/pruning/iceberg_file_pruner.hpp"
#include "planning/scan_plan/iceberg_scan_plan_provider.hpp"
#include "duckdb/storage/table/row_group_reorderer.hpp"

namespace duckdb {

IcebergScanPlanner::IcebergScanPlanner(ClientContext &context_p, shared_ptr<IcebergScanInfo> scan_info,
                                       const string &path, const IcebergOptions &options_p)
    : shared_state(make_shared_ptr<IcebergScanPlanState>(context_p, std::move(scan_info), path, options_p)),
      context(shared_state->context), fs(shared_state->fs), options(shared_state->options) {
}

IcebergScanPlanner::IcebergScanPlanner(shared_ptr<IcebergScanPlanState> shared_state_p)
    : shared_state(std::move(shared_state_p)), context(shared_state->context), fs(shared_state->fs),
      options(shared_state->options) {
}

IcebergScanPlanner::~IcebergScanPlanner() {
}

unique_ptr<IcebergScanPlanner> IcebergScanPlanner::CreateView(IcebergTableFilters filters) const {
	unique_ptr<RowGroupOrderOptions> filtered_scan_order;
	{
		annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
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
	return shared_state->path;
}

const IcebergOptions &IcebergScanPlanner::GetOptions() const {
	return options;
}

const IcebergTableMetadata &IcebergScanPlanner::GetMetadata() const {
	return shared_state->scan_info->metadata;
}

const IcebergTableSchema &IcebergScanPlanner::GetSchema() const {
	return shared_state->scan_info->schema;
}

ClientContext &IcebergScanPlanner::GetContext() const {
	return context;
}

bool IcebergScanPlanner::HasScanInfo() const {
	return shared_state->scan_info != nullptr;
}

bool IcebergScanPlanner::HasTransactionData() const {
	return shared_state->scan_info->transaction_data;
}

const IcebergTransactionData &IcebergScanPlanner::GetTransactionData() const {
	D_ASSERT(HasTransactionData());
	return *shared_state->scan_info->transaction_data;
}

const IcebergSnapshotScanInfo &IcebergScanPlanner::GetSnapshot() const {
	return shared_state->scan_info->snapshot_info;
}

optional_ptr<IcebergTableSchemaVersion> IcebergScanPlanner::GetTable() const {
	return shared_state->table;
}

void IcebergScanPlanner::SetTable(IcebergTableSchemaVersion &table) {
	shared_state->table = table;
}

void IcebergScanPlanner::SetScanInfo(shared_ptr<IcebergScanInfo> scan_info) {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	if (scan_plan_provider) {
		throw InternalException("Cannot replace Iceberg scan info after scan planning has started");
	}
	shared_state->scan_info = std::move(scan_info);
}

void IcebergScanPlanner::SetOptions(const IcebergOptions &new_options) {
	shared_state->options = new_options;
}

void IcebergScanPlanner::SetScanOrder(unique_ptr<RowGroupOrderOptions> order_options) {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	scan_order.Set(std::move(order_options));
}

void IcebergScanPlanner::DisableServerSidePlanning() {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	if (!shared_state->manifest_list_loaded) {
		shared_state->server_side_planning_enabled = false;
	}
}

const IcebergTableFilters &IcebergScanPlanner::Filters() const {
	return table_filters;
}

IcebergScanPlanProvider &IcebergScanPlanner::GetScanPlanProvider() const {
	D_ASSERT(scan_plan_provider);
	return *scan_plan_provider;
}

IcebergScanPlanContext IcebergScanPlanner::GetScanPlanContext() const {
	optional_ptr<const IcebergTransactionData> transaction_data;
	if (HasTransactionData()) {
		transaction_data = &GetTransactionData();
	}
	return {context, fs, GetPath(), options, GetSnapshot(), GetMetadata(), GetSchema(), transaction_data};
}

IcebergDeletePlanningContext IcebergScanPlanner::GetDeletePlanningContext() const {
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

void IcebergScanPlanner::InitializeScanPlanProvider() const {
	if (!scan_plan_provider) {
		scan_plan_provider =
		    IcebergScanPlanProvider::Create(*shared_state, GetScanPlanContext(), GetTable(), table_filters, scan_order,
		                                    shared_state->server_side_planning_enabled);
	}
}

void IcebergScanPlanner::LoadManifestList(annotated_lock_guard<annotated_mutex> &) const {
	InitializeScanPlanProvider();
	GetScanPlanProvider().LoadManifestList();
}

void IcebergScanPlanner::InitializeView(annotated_lock_guard<annotated_mutex> &guard) const {
	if (scan_plan_provider) {
		return;
	}
	LoadManifestList(guard);
	IcebergFilePruner pruner(context, GetMetadata(), GetSchema(), table_filters);
	auto &committed_data = GetScanPlanProvider().DataManifests();
	for (auto &manifest : committed_data) {
		data_manifests.emplace_back(data_manifests.size(), manifest);
		data_manifest_matches.push_back(pruner.ManifestMatchesFilter(manifest.file));
	}
	for (auto &manifest : shared_state->transaction_data_manifests) {
		data_manifests.emplace_back(data_manifests.size(), manifest);
		data_manifest_matches.push_back(pruner.ManifestMatchesFilter(manifest.get().file));
	}
	auto &committed_deletes = GetScanPlanProvider().DeleteManifests();
	bool has_matching_deletes = false;
	for (auto &manifest : committed_deletes) {
		delete_manifests.emplace_back(delete_manifests.size(), manifest);
		auto matches = pruner.ManifestMatchesFilter(manifest.file);
		delete_manifest_matches.push_back(matches);
		has_matching_deletes |= matches;
	}
	for (auto &manifest : shared_state->transaction_delete_manifests) {
		delete_manifests.emplace_back(delete_manifests.size(), manifest);
		auto matches = pruner.ManifestMatchesFilter(manifest.get().file);
		delete_manifest_matches.push_back(matches);
		has_matching_deletes |= matches;
	}
	has_matching_delete_manifests.store(has_matching_deletes);
}

void IcebergScanPlanner::StartDataManifestScan(annotated_lock_guard<annotated_mutex> &) const {
	D_ASSERT(scan_plan_provider);
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
		auto &manifest_file = bound_manifest.entry.file;
		if (!data_manifest_matches[batch.manifest_list_entry_idx]) {
			data_view_cursor.current_batch_offset = batch.end_index;
		}
		for (; data_view_cursor.current_batch_offset < batch.end_index && file_id >= data_manifest_entries.size();
		     data_view_cursor.current_batch_offset++) {
			auto &manifest_entry = manifest_entries[data_view_cursor.current_batch_offset];
			auto &data_file = manifest_entry.data_file;
			auto entry_path = data_file.file_path;
			if (options.allow_moved_paths) {
				entry_path = IcebergUtils::GetFullPath(GetPath(), entry_path, fs);
			}
			IcebergPartition partition {manifest_file.partition_spec_id, data_file.partition_info};
			shared_state->data_file_partitions[entry_path] = partition;
			shared_state->data_file_partitions[data_file.file_path] = std::move(partition);
			auto bound_entry = bound_manifest.BindEntry(manifest_entry);
			if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
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

optional_ptr<const BoundIcebergManifestEntry> IcebergScanPlanner::GetDataFile(idx_t file_id) const {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	GetDataFile(file_id, guard);
	EnsureScanOrderApplied(guard);
	return file_id < data_manifest_entries.size()
	           ? optional_ptr<const BoundIcebergManifestEntry>(data_manifest_entries[file_id])
	           : nullptr;
}

const IcebergManifestFile &IcebergScanPlanner::GetManifestFileForEntry(const BoundIcebergManifestEntry &entry,
                                                                       IcebergManifestContentType type) const {
	return type == IcebergManifestContentType::DATA ? data_manifests[entry.manifest_file_idx].entry.file
	                                                : delete_manifests[entry.manifest_file_idx].entry.file;
}

void IcebergScanPlanner::WithManifestFile(
    const BoundIcebergManifestEntry &entry, IcebergManifestContentType type,
    const std::function<void(const IcebergManifestFile &manifest_file)> &callback) const {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	auto &manifest_file = GetManifestFileForEntry(entry, type);
	callback(manifest_file);
}

optional<IcebergScanTask> IcebergScanPlanner::GetDataFileTask(idx_t file_id) const {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	GetDataFile(file_id, guard);
	EnsureScanOrderApplied(guard);
	if (file_id >= data_manifest_entries.size()) {
		return nullopt;
	}
	auto data_file = data_manifest_entries[file_id];
	auto path = data_file.entry.data_file.file_path;
	if (options.allow_moved_paths) {
		path = IcebergUtils::GetFullPath(GetPath(), path, fs);
	}
	return IcebergScanTask {data_file, std::move(path), {}};
}

optional<IcebergScanTask> IcebergScanPlanner::GetScanTask(idx_t file_id) const {
	auto task = GetDataFileTask(file_id);
	if (task) {
		task->delete_files = ResolveApplicableDeleteFiles(task->manifest_entry);
	}
	return task;
}

idx_t IcebergScanPlanner::GetTotalFileCount() const {
	idx_t file_id = 0;
	while (GetDataFileTask(file_id)) {
		file_id++;
	}
	return file_id;
}

unique_ptr<NodeStatistics> IcebergScanPlanner::GetCardinality() const {
	if (GetMetadata().iceberg_version == 1) {
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
		cardinality += *manifest.counts->added_rows_count + *manifest.counts->existing_rows_count;
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

void IcebergScanPlanner::GetStatistics(vector<PartitionStatistics> &result) const {
	if (GetMetadata().iceberg_version == 1) {
		return;
	}
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	InitializeView(guard);
	for (idx_t i = 0; i < delete_manifests.size(); i++) {
		if (delete_manifest_matches[i]) {
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
		count += *manifest.counts->existing_rows_count + *manifest.counts->added_rows_count;
	}
	PartitionStatistics stats;
	stats.count = count;
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

vector<IcebergDeleteFileReference>
IcebergScanPlanner::ResolveApplicableDeleteFiles(const BoundIcebergManifestEntry &data_manifest_entry) const {
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
	provider->ReadDeleteManifests(manifest_indexes, table_filters.FilterCount());
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	auto delete_context = GetDeletePlanningContext();
	auto partition_values = IcebergFilePruner::PartitionValueMap(data_manifest_entry.entry.data_file);
	for (auto delete_file : provider->GetDeleteFiles(manifest_indexes)) {
		if (delete_file.manifest_idx >= delete_manifests.size()) {
			throw InternalException("Delete manifest index %llu is out of bounds for %llu manifests",
			                        delete_file.manifest_idx, delete_manifests.size());
		}
		auto &entries = delete_manifests[delete_file.manifest_idx].entry.GetManifestEntries();
		if (delete_file.entry_idx >= entries.size()) {
			throw InternalException("Delete manifest entry index %llu is out of bounds for manifest %llu",
			                        delete_file.entry_idx, delete_file.manifest_idx);
		}
		auto &delete_entry = entries[delete_file.entry_idx];
		if (IcebergDeletePlanner::DeleteEntryMatchesFilters(delete_context, delete_file.manifest_idx, delete_entry) &&
		    IcebergDeletePlanner::DeleteEntryAppliesToDataFile(delete_context, delete_file.manifest_idx, delete_entry,
		                                                       data_manifest_entry, partition_values)) {
			result.push_back(delete_file);
		}
	}
	return result;
}

const IcebergManifestListEntry &IcebergScanPlanner::GetDeleteManifest(IcebergDeleteFileReference delete_file) const {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	if (delete_file.manifest_idx >= delete_manifests.size()) {
		throw InternalException("Delete manifest index %llu is out of bounds", delete_file.manifest_idx);
	}
	auto &manifest = delete_manifests[delete_file.manifest_idx].entry;
	if (delete_file.entry_idx >= manifest.GetManifestEntries().size()) {
		throw InternalException("Delete manifest entry index %llu is out of bounds for manifest %llu",
		                        delete_file.entry_idx, delete_file.manifest_idx);
	}
	return manifest;
}

unique_ptr<IcebergDeletePlanningContext> IcebergScanPlanner::CreateDeletePlanningContext() const {
	annotated_lock_guard<annotated_mutex> guard(shared_state->lock);
	return make_uniq<IcebergDeletePlanningContext>(GetDeletePlanningContext());
}

} // namespace duckdb
