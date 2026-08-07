#include "planning/scan_plan/iceberg_scan_plan_provider.hpp"

#include "planning/iceberg_multi_file_list.hpp"
#include "planning/pruning/iceberg_table_filter.hpp"
#include "planning/scan_order/iceberg_scan_order.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/api/iceberg_expression.hpp"
#include "catalog/rest/api/iceberg_type.hpp"
#include "common/iceberg_utils.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parallel/task_notifier.hpp"
#include "duckdb/storage/table/row_group_reorderer.hpp"
#include "iceberg_logging.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/metadata_io/manifest_list/iceberg_manifest_list_reader.hpp"

#include <condition_variable>

namespace duckdb {

struct IcebergDeleteManifestLoadState {
	mutex lock;
	std::condition_variable cv;
	bool complete = false;
	ErrorData error;
	vector<idx_t> manifest_indexes;
	vector<IcebergManifestListEntry> manifests;
	shared_ptr<IcebergManifestScanningState> scan_state;
};

namespace {

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

ClientSideScanPlanProvider::ClientSideScanPlanProvider(IcebergScanPlanState &shared_state_p,
                                                       IcebergScanPlanContext context_p)
    : shared_state(shared_state_p), context(std::move(context_p)) {
}

void ClientSideScanPlanProvider::LoadManifestList() {
	if (shared_state.manifest_list_loaded) {
		return;
	}

	auto &snapshot_info = context.snapshot;
	if (snapshot_info.snapshot) {
		auto &iceberg_path = context.path;
		auto &snapshot = *snapshot_info.snapshot;
		auto &metadata = context.metadata;
		auto &fs = context.fs;

		vector<IcebergManifestListEntry> manifest_list_entries;
		if (context.transaction_data && !context.transaction_data->alters.empty()) {
			manifest_list_entries = context.transaction_data->existing_manifest_list;
		} else {
			if (!snapshot.manifests.empty()) {
				IcebergManifestList::LoadManifestFiles(snapshot_info, metadata, context.context, manifest_list_entries);
			} else {
				auto manifest_list_full_path = context.options.allow_moved_paths
				                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
				                                   : snapshot.manifest_list;
				auto scan = AvroScan::ScanManifestList(snapshot_info, metadata, context.context,
				                                       manifest_list_full_path, manifest_list_entries);
				auto manifest_list_reader = make_uniq<manifest_list::ManifestListReader>(*scan);
				while (!manifest_list_reader->Finished()) {
					manifest_list_reader->Read();
				}
			}
		}

		for (auto &manifest_list_entry : manifest_list_entries) {
			if (manifest_list_entry.file.content == IcebergManifestContentType::DATA) {
				DataManifests().push_back(std::move(manifest_list_entry));
			} else {
				D_ASSERT(manifest_list_entry.file.content == IcebergManifestContentType::DELETE);
				DeleteManifests().push_back(std::move(manifest_list_entry));
			}
		}

		auto &data_manifests = DataManifests();
		shared_state.eagerly_loaded_data_manifests.resize(data_manifests.size(), false);
		vector<idx_t> manifests_to_eagerly_load;
		for (idx_t manifest_idx = 0; manifest_idx < data_manifests.size(); manifest_idx++) {
			auto &manifest = data_manifests[manifest_idx];
			if (manifest.HasManifestEntries()) {
				shared_state.eagerly_loaded_data_manifests[manifest_idx] = true;
				if (!manifest.file.counts || !manifest.file.counts->Complete()) {
					manifest.file.SetCountsFromEntries(manifest.GetManifestEntries());
				}
				continue;
			}

			auto &counts = manifest.file.counts;
			if (!counts || !counts->FilesComplete()) {
				manifests_to_eagerly_load.push_back(manifest_idx);
				continue;
			}

			idx_t reserve_size =
			    *counts->existing_files_count + *counts->added_files_count + *counts->deleted_files_count;
			manifest.GetOrCreateManifestEntries().reserve(reserve_size);
		}

		if (!manifests_to_eagerly_load.empty()) {
			auto scan =
			    AvroScan::ScanManifest(context.snapshot, data_manifests, context.options, context.fs, context.path,
			                           context.metadata, context.context, nullptr, manifests_to_eagerly_load);
			auto reader = make_uniq<manifest_file::ManifestReader>(*scan);
			while (!reader->Finished()) {
				reader->Read();
			}
			for (auto manifest_idx : manifests_to_eagerly_load) {
				auto &manifest = data_manifests[manifest_idx];
				manifest.file.SetCountsFromEntries(manifest.GetManifestEntries());
				shared_state.eagerly_loaded_data_manifests[manifest_idx] = true;
			}
		}
	}

	if (context.transaction_data) {
		for (auto &alter_p : context.transaction_data->alters) {
			for (auto &manifest_list_entry : alter_p.get().GetManifestFiles()) {
				switch (manifest_list_entry.file.content) {
				case IcebergManifestContentType::DATA:
					shared_state.transaction_data_manifests.push_back(manifest_list_entry);
					break;
				case IcebergManifestContentType::DELETE:
					shared_state.transaction_delete_manifests.push_back(manifest_list_entry);
					break;
				default:
					throw NotImplementedException("IcebergManifestContentType: %d",
					                              static_cast<uint8_t>(manifest_list_entry.file.content));
				}
			}
		}
	}

	{
		annotated_lock_guard<annotated_mutex> delete_guard(shared_state.delete_lock);
		shared_state.delete_manifest_loads.resize(DeleteManifests().size());
		shared_state.delete_file_loads.resize(DeleteManifests().size() +
		                                      shared_state.transaction_delete_manifests.size());
	}

	shared_state.manifest_list_loaded = true;
	DUCKDB_LOG(shared_state.context, IcebergLogType,
	           "Iceberg metadata phase=manifest_list_loaded data_manifests=%llu delete_manifests=%llu",
	           DataManifests().size() + shared_state.transaction_data_manifests.size(),
	           DeleteManifests().size() + shared_state.transaction_delete_manifests.size());
}

void ClientSideScanPlanProvider::StartDataManifestScan(const vector<bool> &matching_manifests, idx_t filter_count) {
	if (shared_state.data_manifest_scan_started) {
		return;
	}
	shared_state.data_manifest_scan_started = true;

	const auto committed_manifest_count = DataManifests().size();
	vector<idx_t> selected_committed_manifests;
	for (idx_t manifest_idx = 0; manifest_idx < committed_manifest_count; manifest_idx++) {
		if (!matching_manifests[manifest_idx]) {
			continue;
		}
		if (shared_state.eagerly_loaded_data_manifests[manifest_idx]) {
			auto &manifest = DataManifests()[manifest_idx];
			shared_state.read_state.PushBatch(
			    ManifestReadBatch {manifest_idx, 0, manifest.GetManifestEntries().size()});
		} else {
			selected_committed_manifests.push_back(manifest_idx);
		}
	}

	for (idx_t transaction_idx = 0; transaction_idx < shared_state.transaction_data_manifests.size();
	     transaction_idx++) {
		auto manifest_idx = committed_manifest_count + transaction_idx;
		if (!matching_manifests[manifest_idx]) {
			continue;
		}
		auto &manifest = shared_state.transaction_data_manifests[transaction_idx].get();
		shared_state.read_state.PushBatch(ManifestReadBatch {manifest_idx, 0, manifest.GetManifestEntries().size()});
	}

	if (!selected_committed_manifests.empty()) {
		auto data_scan = AvroScan::ScanManifest(context.snapshot, DataManifests(), context.options, context.fs,
		                                        context.path, context.metadata, context.context,
		                                        &shared_state.read_state, selected_committed_manifests);
		shared_state.data_manifest_read_state =
		    make_uniq<IcebergManifestScanningState>(shared_state.context, std::move(data_scan), DataManifests());

		auto &executor = shared_state.data_manifest_read_state->executor;
		auto &scheduler = TaskScheduler::GetScheduler(shared_state.context);
		auto num_threads = MinValue<idx_t>(scheduler.NumberOfThreads(), selected_committed_manifests.size());
		shared_state.data_manifest_read_state->in_progress_tasks = num_threads;
		for (idx_t i = 0; i < num_threads; i++) {
			executor.ScheduleTask(make_uniq<ManifestReadTask>(*shared_state.data_manifest_read_state));
		}
	}

	idx_t selected_manifest_count = 0;
	for (auto matches : matching_manifests) {
		selected_manifest_count += matches;
	}
	DUCKDB_LOG(shared_state.context, IcebergLogType,
	           "Iceberg metadata phase=data_manifest_scan_started selected_data_manifests=%llu "
	           "total_data_manifests=%llu filters=%llu",
	           selected_manifest_count, matching_manifests.size(), filter_count);
}

void ClientSideScanPlanProvider::ReadDeleteManifests(const vector<idx_t> &manifest_indexes, idx_t filter_count) {
	shared_ptr<IcebergDeleteManifestLoadState> new_load;
	vector<shared_ptr<IcebergDeleteManifestLoadState>> required_loads;
	idx_t committed_manifest_count;
	{
		annotated_lock_guard<annotated_mutex> guard(shared_state.lock);
		annotated_lock_guard<annotated_mutex> delete_guard(shared_state.delete_lock);
		committed_manifest_count = shared_state.committed_delete_manifests.size();
		auto total_manifest_count = committed_manifest_count + shared_state.transaction_delete_manifests.size();
		for (auto manifest_idx : manifest_indexes) {
			if (manifest_idx >= total_manifest_count) {
				throw InternalException("Selected delete manifest index %llu is out of bounds", manifest_idx);
			}
			if (manifest_idx >= committed_manifest_count) {
				continue;
			}
			auto &manifest = shared_state.committed_delete_manifests[manifest_idx];
			if (manifest.HasManifestEntries()) {
				//! Scan for this manifest is already completed
				continue;
			}
			auto &load = shared_state.delete_manifest_loads[manifest_idx];
			if (!load) {
				//! No load for this manifest exists yet
				if (!new_load) {
					//! We haven't created a load yet, initialize it here
					new_load = make_shared_ptr<IcebergDeleteManifestLoadState>();
					required_loads.push_back(new_load);
				}
				//! Set the load for this manifest to indicate it's being loaded
				load = new_load;
				new_load->manifest_indexes.push_back(manifest_idx);
				new_load->manifests.push_back(manifest);
			} else {
				//! A load already exists for this manifest
				bool already_required = false;
				for (auto &required_load : required_loads) {
					if (required_load.get() == load.get()) {
						//! We've already registered a previous manifest that is part of the same load
						already_required = true;
						break;
					}
				}
				if (!already_required) {
					//! We haven't seen this load yet, add it to our required loads
					required_loads.push_back(load);
				}
			}
		}
	}

	if (new_load) {
		//! At least one of the manifests we need aren't referenced yet, need to start a scan for it/them
		ErrorData load_error;
		try {
			auto scan = AvroScan::ScanManifest(context.snapshot, new_load->manifests, context.options, context.fs,
			                                   context.path, context.metadata, context.context);
			new_load->scan_state = make_shared_ptr<IcebergManifestScanningState>(shared_state.context, std::move(scan),
			                                                                     new_load->manifests);

			auto &executor = new_load->scan_state->executor;
			auto &scheduler = TaskScheduler::GetScheduler(shared_state.context);
			auto num_threads = MinValue<idx_t>(scheduler.NumberOfThreads(), new_load->manifest_indexes.size());
			new_load->scan_state->in_progress_tasks = num_threads;
			for (idx_t i = 0; i < num_threads; i++) {
				executor.ScheduleTask(make_uniq<ManifestReadTask>(*new_load->scan_state));
			}

			DUCKDB_LOG(shared_state.context, IcebergLogType,
			           "Iceberg metadata phase=delete_manifest_scan_started selected_delete_manifests=%llu "
			           "total_delete_manifests=%llu filters=%llu",
			           new_load->manifest_indexes.size(), committed_manifest_count, filter_count);
			executor.WorkOnTasks();

			annotated_lock_guard<annotated_mutex> guard(shared_state.lock);
			for (idx_t load_idx = 0; load_idx < new_load->manifest_indexes.size(); load_idx++) {
				auto manifest_idx = new_load->manifest_indexes[load_idx];
				auto &target = shared_state.committed_delete_manifests[manifest_idx];
				auto &source = new_load->manifests[load_idx];
				D_ASSERT(!target.HasManifestEntries());
				target.manifest_entries = std::move(source.manifest_entries);
			}
		} catch (std::exception &ex) {
			load_error = ErrorData(ex);
		} catch (...) { // LCOV_EXCL_START
			load_error = ErrorData("Unknown exception while reading Iceberg delete manifests");
		} // LCOV_EXCL_STOP

		{
			lock_guard<mutex> guard(new_load->lock);
			new_load->error = std::move(load_error);
			new_load->complete = true;
		}
		new_load->cv.notify_all();
	}

	for (auto &load : required_loads) {
		unique_lock<mutex> guard(load->lock);
		load->cv.wait(guard, [&load] { return load->complete; });
		if (load->error.HasError()) {
			load->error.Throw();
		}
	}
}

vector<IcebergDeleteFileReference> ClientSideScanPlanProvider::GetDeleteFiles(const vector<idx_t> &manifest_indexes) {
	vector<IcebergDeleteFileReference> result;
	auto committed_manifest_count = DeleteManifests().size();
	auto total_manifest_count = committed_manifest_count + shared_state.transaction_delete_manifests.size();
	for (auto manifest_idx : manifest_indexes) {
		if (manifest_idx >= total_manifest_count) {
			throw InternalException("Selected delete manifest index %llu is out of bounds", manifest_idx);
		}

		if (manifest_idx < committed_manifest_count) {
			auto &manifest_list_entry = DeleteManifests()[manifest_idx];
			if (!manifest_list_entry.HasManifestEntries()) {
				throw InternalException("Selected delete manifest %llu was not loaded", manifest_idx);
			}
			auto &manifest_entries = manifest_list_entry.GetManifestEntries();
			for (idx_t entry_idx = 0; entry_idx < manifest_entries.size(); entry_idx++) {
				auto &manifest_entry = manifest_entries[entry_idx];
				if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
					continue;
				}
				if (context.transaction_data &&
				    context.transaction_data->IsFileInvalidated(manifest_entry.data_file.file_path)) {
					continue;
				}
				result.push_back({manifest_idx, entry_idx});
			}
		} else {
			auto transaction_idx = manifest_idx - committed_manifest_count;
			auto &manifest_list_entry = shared_state.transaction_delete_manifests[transaction_idx].get();
			auto &manifest_entries = manifest_list_entry.GetManifestEntries();
			for (idx_t entry_idx = 0; entry_idx < manifest_entries.size(); entry_idx++) {
				auto &manifest_entry = manifest_entries[entry_idx];
				if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
					continue;
				}
				if (context.transaction_data &&
				    context.transaction_data->IsFileInvalidated(manifest_entry.data_file.file_path)) {
					continue;
				}
				result.push_back({manifest_idx, entry_idx});
			}
		}
	}
	return result;
}

bool ClientSideScanPlanProvider::TryGetNextBatch(IcebergDataViewCursor &cursor) {
	if (cursor.has_current_batch || shared_state.read_state.TryReadBatch(cursor)) {
		return true;
	}
	if (!shared_state.data_manifest_read_state) {
		return false;
	}
	auto &scheduler = TaskScheduler::GetScheduler(shared_state.context);
	auto &scan_state = *shared_state.data_manifest_read_state;
	auto &executor = scan_state.executor;
	shared_ptr<Task> task_to_execute;
	while (scan_state.in_progress_tasks) {
		if (executor.GetTask(task_to_execute)) {
			auto res = task_to_execute->Execute(TaskExecutionMode::PROCESS_PARTIAL);
			if (res == TaskExecutionResult::TASK_NOT_FINISHED) {
				auto &token = *task_to_execute->token;
				scheduler.ScheduleTask(token, std::move(task_to_execute));
			}
			if (shared_state.read_state.TryReadBatch(cursor)) {
				return true;
			}
		}
		executor.WorkOnTasks();
		break;
	}
	return shared_state.read_state.TryReadBatch(cursor);
}

void ClientSideScanPlanProvider::FinishScanTasks() {
	if (shared_state.data_manifest_read_state) {
		shared_state.data_manifest_read_state->executor.WorkOnTasks();
	}
}

bool ClientSideScanPlanProvider::DeleteFileAppliesToDataFile(const string &data_file_path,
                                                             const string &delete_file_path) const {
	return true;
}

vector<IcebergManifestListEntry> &ClientSideScanPlanProvider::DataManifests() {
	return shared_state.committed_data_manifests;
}

vector<IcebergManifestListEntry> &ClientSideScanPlanProvider::DeleteManifests() {
	return shared_state.committed_delete_manifests;
}

shared_ptr<IcebergDeleteFileLoadState> &
ClientSideScanPlanProvider::GetDeleteFileLoad(IcebergDeleteFileReference delete_file) {
	auto committed_manifest_count = DeleteManifests().size();
	auto total_manifest_count = committed_manifest_count + shared_state.transaction_delete_manifests.size();
	if (delete_file.manifest_idx >= total_manifest_count) {
		throw InternalException("Delete manifest index %llu is out of bounds", delete_file.manifest_idx);
	}
	auto &manifest =
	    delete_file.manifest_idx < committed_manifest_count
	        ? DeleteManifests()[delete_file.manifest_idx]
	        : shared_state.transaction_delete_manifests[delete_file.manifest_idx - committed_manifest_count].get();
	auto &manifest_entries = manifest.GetManifestEntries();
	if (delete_file.entry_idx >= manifest_entries.size()) {
		throw InternalException("Delete manifest entry index %llu is out of bounds for manifest %llu",
		                        delete_file.entry_idx, delete_file.manifest_idx);
	}
	auto &loads = shared_state.delete_file_loads[delete_file.manifest_idx];
	return loads[delete_file.entry_idx];
}

position_delete_map_t &ClientSideScanPlanProvider::PositionalDeleteData() {
	return shared_state.positional_delete_data;
}

} // namespace duckdb
