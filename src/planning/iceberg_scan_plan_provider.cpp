#include "planning/iceberg_scan_plan_provider.hpp"

#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "common/iceberg_utils.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parallel/task_notifier.hpp"
#include "iceberg_logging.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/metadata_io/manifest_list/bound_iceberg_manifest_list_entry.hpp"
#include "planning/metadata_io/manifest_list/iceberg_manifest_list_reader.hpp"

namespace duckdb {

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

static bool TryReadBatch(ManifestEntryReadState &read_state, IcebergDataViewCursor &cursor) {
	if (!read_state.GetBatch(cursor.next_batch_idx, cursor.current_batch)) {
		return false;
	}
	cursor.next_batch_idx++;
	cursor.current_batch_offset = cursor.current_batch.start_index;
	cursor.has_current_batch = true;
	return true;
}

} // namespace

ClientSideScanPlanProvider::ClientSideScanPlanProvider(IcebergMultiFileListSharedState &shared_state_p)
    : shared_state(shared_state_p) {
}

void ClientSideScanPlanProvider::LoadManifestList(const IcebergMultiFileList &file_list) {
	if (shared_state.manifest_list_loaded) {
		return;
	}

	auto &snapshot_info = shared_state.scan_info->snapshot_info;
	if (snapshot_info.snapshot) {
		auto iceberg_path = file_list.GetPath();
		auto &snapshot = *snapshot_info.snapshot;
		auto &metadata = file_list.GetMetadata();
		auto &fs = FileSystem::GetFileSystem(shared_state.context);

		vector<IcebergManifestListEntry> manifest_list_entries;
		if (file_list.HasTransactionData() && !file_list.GetTransactionData().alters.empty()) {
			manifest_list_entries = file_list.GetTransactionData().existing_manifest_list;
		} else {
			auto manifest_list_full_path = file_list.options.allow_moved_paths
			                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
			                                   : snapshot.manifest_list;
			auto scan = AvroScan::ScanManifestList(snapshot_info, metadata, shared_state.context,
			                                       manifest_list_full_path, manifest_list_entries);
			auto manifest_list_reader = make_uniq<manifest_list::ManifestListReader>(*scan);
			while (!manifest_list_reader->Finished()) {
				manifest_list_reader->Read();
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

		for (auto &manifest : DataManifests()) {
			if (!manifest.HasManifestEntries()) {
				continue;
			}
			auto &file = manifest.file;
			idx_t reserve_size = file.existing_files_count + file.added_files_count + file.deleted_files_count;
			manifest.GetManifestEntries().reserve(reserve_size);
		}
	}

	if (file_list.HasTransactionData()) {
		for (auto &alter_p : file_list.GetTransactionData().alters) {
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

	shared_state.manifest_list_loaded = true;
	DUCKDB_LOG(shared_state.context, IcebergLogType,
	           "Iceberg metadata phase=manifest_list_loaded data_manifests=%llu delete_manifests=%llu",
	           DataManifests().size() + shared_state.transaction_data_manifests.size(),
	           DeleteManifests().size() + shared_state.transaction_delete_manifests.size());
}

void ClientSideScanPlanProvider::StartDeleteManifestScan(const IcebergMultiFileList &file_list) {
	if (shared_state.delete_manifest_scan || DeleteManifests().empty()) {
		return;
	}
	shared_state.delete_manifest_scan =
	    AvroScan::ScanManifest(file_list.GetSnapshot(), DeleteManifests(), file_list.options, shared_state.fs,
	                           file_list.GetPath(), file_list.GetMetadata(), shared_state.context);
	shared_state.delete_manifest_reader = make_uniq<manifest_file::ManifestReader>(*shared_state.delete_manifest_scan);
}

void ClientSideScanPlanProvider::StartDataManifestScan(const IcebergMultiFileList &file_list) {
	if (shared_state.data_manifest_scan_started) {
		return;
	}
	shared_state.data_manifest_scan_started = true;

	const auto committed_manifest_count = DataManifests().size();
	vector<idx_t> selected_committed_manifests;
	for (idx_t manifest_idx = 0; manifest_idx < committed_manifest_count; manifest_idx++) {
		if (file_list.data_manifest_matches[manifest_idx]) {
			selected_committed_manifests.push_back(manifest_idx);
		}
	}

	for (idx_t transaction_idx = 0; transaction_idx < shared_state.transaction_data_manifests.size();
	     transaction_idx++) {
		auto manifest_idx = committed_manifest_count + transaction_idx;
		if (!file_list.data_manifest_matches[manifest_idx]) {
			continue;
		}
		auto &manifest = shared_state.transaction_data_manifests[transaction_idx].get();
		shared_state.read_state.PushBatch(ManifestReadBatch {manifest_idx, 0, manifest.GetManifestEntries().size()});
	}

	if (!selected_committed_manifests.empty()) {
		auto data_scan = AvroScan::ScanManifest(
		    file_list.GetSnapshot(), DataManifests(), file_list.options, shared_state.fs, file_list.GetPath(),
		    file_list.GetMetadata(), shared_state.context, &shared_state.read_state, selected_committed_manifests);
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
	for (auto matches : file_list.data_manifest_matches) {
		selected_manifest_count += matches;
	}
	DUCKDB_LOG(shared_state.context, IcebergLogType,
	           "Iceberg metadata phase=data_manifest_scan_started selected_data_manifests=%llu "
	           "total_data_manifests=%llu filters=%llu",
	           selected_manifest_count, file_list.data_manifest_matches.size(), file_list.table_filters.FilterCount());
}

void ClientSideScanPlanProvider::EnumerateDeleteManifestEntries(const IcebergMultiFileList &file_list) {
	if (shared_state.delete_entries_enumerated) {
		return;
	}
	StartDeleteManifestScan(file_list);

	optional_ptr<const case_insensitive_map_t<string>> transactional_delete_files;
	if (file_list.HasTransactionData()) {
		transactional_delete_files = file_list.GetTransactionData().transactional_delete_files;
	}
	while (!FinishedScanningDeletes()) {
		shared_state.delete_manifest_reader->Read();
	}

	for (idx_t i = 0; i < DeleteManifests().size(); i++) {
		auto &manifest_list_entry = DeleteManifests()[i];
		auto manifest = BoundIcebergManifestListEntry(i, manifest_list_entry);
		for (auto &manifest_entry : manifest_list_entry.GetManifestEntries()) {
			if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
				continue;
			}
			auto &referenced_data_file = manifest_entry.data_file.referenced_data_file;
			if (referenced_data_file && transactional_delete_files &&
			    transactional_delete_files->count(*referenced_data_file)) {
				continue;
			}
			shared_state.delete_manifest_entries.push_back(manifest.BindEntry(manifest_entry));
		}
	}

	auto offset = DeleteManifests().size();
	for (idx_t transaction_idx = 0; transaction_idx < shared_state.transaction_delete_manifests.size();
	     transaction_idx++) {
		auto &manifest_list_entry = shared_state.transaction_delete_manifests[transaction_idx].get();
		auto manifest = BoundIcebergManifestListEntry(offset + transaction_idx, manifest_list_entry);
		for (auto &manifest_entry : manifest_list_entry.GetManifestEntries()) {
			auto &data_file = manifest_entry.data_file;
			auto &referenced_data_file = data_file.referenced_data_file;
			if (referenced_data_file && transactional_delete_files) {
				auto it = transactional_delete_files->find(*referenced_data_file);
				if (it != transactional_delete_files->end() && it->second != data_file.file_path) {
					continue;
				}
			}
			shared_state.delete_manifest_entries.push_back(manifest.BindEntry(manifest_entry));
		}
	}

	shared_state.delete_entries_enumerated = true;
	D_ASSERT(FinishedScanningDeletes());
}

bool ClientSideScanPlanProvider::TryGetNextBatch(IcebergDataViewCursor &cursor) {
	if (cursor.has_current_batch || TryReadBatch(shared_state.read_state, cursor)) {
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
			if (TryReadBatch(shared_state.read_state, cursor)) {
				return true;
			}
		}
		executor.WorkOnTasks();
		break;
	}
	return TryReadBatch(shared_state.read_state, cursor);
}

void ClientSideScanPlanProvider::FinishScanTasks() {
	if (shared_state.data_manifest_read_state) {
		shared_state.data_manifest_read_state->executor.WorkOnTasks();
	}
}

bool ClientSideScanPlanProvider::FinishedScanningDeletes() const {
	return !shared_state.delete_manifest_reader || shared_state.delete_manifest_reader->Finished();
}

vector<IcebergManifestListEntry> &ClientSideScanPlanProvider::DataManifests() {
	return shared_state.committed_data_manifests;
}

vector<IcebergManifestListEntry> &ClientSideScanPlanProvider::DeleteManifests() {
	return shared_state.committed_delete_manifests;
}

idx_t &ClientSideScanPlanProvider::NextDeleteEntryToProcess() {
	return shared_state.next_delete_entry_to_process;
}

vector<BoundIcebergManifestEntry> &ClientSideScanPlanProvider::DeleteManifestEntries() {
	return shared_state.delete_manifest_entries;
}

case_insensitive_map_t<shared_ptr<IcebergDeleteData>> &ClientSideScanPlanProvider::PositionalDeleteData() {
	return shared_state.positional_delete_data;
}

map<sequence_number_t, unique_ptr<IcebergEqualityDeleteData>> &ClientSideScanPlanProvider::EqualityDeleteData() {
	return shared_state.equality_delete_data;
}

} // namespace duckdb
