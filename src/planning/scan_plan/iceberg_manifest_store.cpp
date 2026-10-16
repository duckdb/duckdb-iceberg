#include "planning/scan_plan/iceberg_manifest_store.hpp"

#include "common/iceberg_utils.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "iceberg_logging.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/metadata_io/manifest_list/iceberg_manifest_list_reader.hpp"

#include <condition_variable>

namespace duckdb {

struct IcebergManifestScanningState {
	IcebergManifestScanningState(ClientContext &context, unique_ptr<AvroScan> scan,
	                             vector<IcebergManifestListEntry> &list_entries)
	    : context(context), executor(context), scan(std::move(scan)), list_entries(list_entries), in_progress_tasks(0) {
	}

	ClientContext &context;
	TaskExecutor executor;
	unique_ptr<AvroScan> scan;
	vector<IcebergManifestListEntry> &list_entries;
	atomic<idx_t> in_progress_tasks;
};

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

	~ManifestReadTask() override {
		AccountFinished();
	}

	//! One manifest read per step. The wrapper loops this while draining and yields between steps.
	TaskExecutionResult ExecuteTaskStep() override {
		if (!reader.Finished()) {
			reader.Read();
			return TaskExecutionResult::TASK_NOT_FINISHED;
		}
		AccountFinished();
		return TaskExecutionResult::TASK_FINISHED;
	}

	void Cancel() override {
		AccountFinished();
	}

	string TaskType() const override {
		return "IcebergManifestReadTask";
	}

private:
	void AccountFinished() {
		if (!accounted.exchange(true)) {
			--state.in_progress_tasks;
		}
	}

	IcebergManifestScanningState &state;
	manifest_file::ManifestReader reader;
	atomic<bool> accounted {false};
};

} // namespace

IcebergManifestStore::IcebergManifestStore(annotated_mutex &lock_p, IcebergScanPlanContext context_p)
    : lock(lock_p), context(std::move(context_p)) {
}

void IcebergManifestStore::LoadManifestList() {
	if (manifest_list_loaded) {
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
			auto content = manifest_list_entry.GetFile().content;
			if (content == IcebergManifestContentType::DATA) {
				committed_data_manifests.push_back(std::move(manifest_list_entry));
			} else {
				D_ASSERT(content == IcebergManifestContentType::DELETE);
				committed_delete_manifests.push_back(std::move(manifest_list_entry));
			}
		}

		auto &data_manifests = committed_data_manifests;
		eagerly_loaded_data_manifests.resize(data_manifests.size(), false);
		vector<idx_t> manifests_to_eagerly_load;
		for (idx_t manifest_idx = 0; manifest_idx < data_manifests.size(); manifest_idx++) {
			auto &list_entry = data_manifests[manifest_idx];
			auto &manifest = list_entry.GetManifest();
			if (list_entry.HasManifestEntries()) {
				eagerly_loaded_data_manifests[manifest_idx] = true;
				if (!manifest.counts || !manifest.counts->Complete()) {
					manifest.SetCountsFromEntries(list_entry.GetManifestEntries());
				}
				continue;
			}

			auto &counts = manifest.counts;
			if (!counts || !counts->FilesComplete()) {
				manifests_to_eagerly_load.push_back(manifest_idx);
				continue;
			}

			idx_t reserve_size =
			    *counts->existing_files_count + *counts->added_files_count + *counts->deleted_files_count;
			list_entry.GetOrCreateManifestEntries().reserve(reserve_size);
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
				manifest.GetManifest().SetCountsFromEntries(manifest.GetManifestEntries());
				eagerly_loaded_data_manifests[manifest_idx] = true;
			}
		}
	}

	if (context.transaction_data) {
		auto sequence_number = context.transaction_data->scan_sequence_number;
		for (auto &alter_p : context.transaction_data->alters) {
			for (const auto &pending_manifest : alter_p.get().GetPendingManifests()) {
				auto manifest_list_entry = pending_manifest.CreateScanEntry(context.metadata, sequence_number);
				auto content = pending_manifest.GetMetadata().content;
				switch (content) {
				case IcebergManifestContentType::DATA:
					transaction_data_manifests.push_back(std::move(manifest_list_entry));
					break;
				case IcebergManifestContentType::DELETE:
					transaction_delete_manifests.push_back(std::move(manifest_list_entry));
					break;
				default:
					throw NotImplementedException("IcebergManifestContentType: %d", static_cast<uint8_t>(content));
				}
			}
			sequence_number++;
		}
	}

	{
		annotated_lock_guard<annotated_mutex> delete_guard(delete_manifest_lock);
		delete_manifest_loads.resize(committed_delete_manifests.size());
	}

	manifest_list_loaded = true;
	DUCKDB_LOG(context.context, IcebergLogType,
	           "Iceberg metadata phase=manifest_list_loaded data_manifests=%llu delete_manifests=%llu",
	           committed_data_manifests.size() + transaction_data_manifests.size(),
	           committed_delete_manifests.size() + transaction_delete_manifests.size());
}

void IcebergManifestStore::StartDataManifestScan(const vector<bool> &matching_manifests, idx_t filter_count) {
	if (data_manifest_scan_started) {
		return;
	}
	data_manifest_scan_started = true;

	const auto committed_manifest_count = committed_data_manifests.size();
	vector<idx_t> selected_committed_manifests;
	for (idx_t manifest_idx = 0; manifest_idx < committed_manifest_count; manifest_idx++) {
		if (!matching_manifests[manifest_idx]) {
			continue;
		}
		if (eagerly_loaded_data_manifests[manifest_idx]) {
			auto &manifest = committed_data_manifests[manifest_idx];
			read_state.PushBatch(ManifestReadBatch {manifest_idx, 0, manifest.GetManifestEntries().size()});
		} else {
			selected_committed_manifests.push_back(manifest_idx);
		}
	}

	for (idx_t transaction_idx = 0; transaction_idx < transaction_data_manifests.size(); transaction_idx++) {
		auto manifest_idx = committed_manifest_count + transaction_idx;
		if (!matching_manifests[manifest_idx]) {
			continue;
		}
		auto &manifest = transaction_data_manifests[transaction_idx];
		read_state.PushBatch(ManifestReadBatch {manifest_idx, 0, manifest.GetManifestEntries().size()});
	}

	if (!selected_committed_manifests.empty()) {
		auto data_scan = AvroScan::ScanManifest(context.snapshot, committed_data_manifests, context.options, context.fs,
		                                        context.path, context.metadata, context.context, &read_state,
		                                        selected_committed_manifests);
		data_manifest_read_state =
		    make_uniq<IcebergManifestScanningState>(context.context, std::move(data_scan), committed_data_manifests);

		auto &executor = data_manifest_read_state->executor;
		auto &scheduler = TaskScheduler::GetScheduler(context.context);
		auto num_threads = MinValue<idx_t>(scheduler.NumberOfThreads(), selected_committed_manifests.size());
		data_manifest_read_state->in_progress_tasks = num_threads;
		for (idx_t i = 0; i < num_threads; i++) {
			executor.ScheduleTask(make_uniq<ManifestReadTask>(*data_manifest_read_state));
		}
	}

	idx_t selected_manifest_count = 0;
	for (auto matches : matching_manifests) {
		selected_manifest_count += matches;
	}
	DUCKDB_LOG(context.context, IcebergLogType,
	           "Iceberg metadata phase=data_manifest_scan_started selected_data_manifests=%llu "
	           "total_data_manifests=%llu filters=%llu",
	           selected_manifest_count, matching_manifests.size(), filter_count);
}

void IcebergManifestStore::ReadDeleteManifests(const vector<idx_t> &manifest_indexes, idx_t filter_count) {
	shared_ptr<IcebergDeleteManifestLoadState> new_load;
	vector<shared_ptr<IcebergDeleteManifestLoadState>> required_loads;
	idx_t committed_manifest_count;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		annotated_lock_guard<annotated_mutex> delete_guard(delete_manifest_lock);
		committed_manifest_count = committed_delete_manifests.size();
		auto total_manifest_count = committed_manifest_count + transaction_delete_manifests.size();
		for (auto manifest_idx : manifest_indexes) {
			if (manifest_idx >= total_manifest_count) {
				throw InternalException("Selected delete manifest index %llu is out of bounds", manifest_idx);
			}
			if (manifest_idx >= committed_manifest_count) {
				continue;
			}
			auto &manifest = committed_delete_manifests[manifest_idx];
			if (manifest.HasManifestEntries()) {
				//! Scan for this manifest is already completed
				continue;
			}
			auto &load = delete_manifest_loads[manifest_idx];
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
			new_load->scan_state =
			    make_shared_ptr<IcebergManifestScanningState>(context.context, std::move(scan), new_load->manifests);

			auto &executor = new_load->scan_state->executor;
			auto &scheduler = TaskScheduler::GetScheduler(context.context);
			auto num_threads = MinValue<idx_t>(scheduler.NumberOfThreads(), new_load->manifest_indexes.size());
			new_load->scan_state->in_progress_tasks = num_threads;
			for (idx_t i = 0; i < num_threads; i++) {
				executor.ScheduleTask(make_uniq<ManifestReadTask>(*new_load->scan_state));
			}

			DUCKDB_LOG(context.context, IcebergLogType,
			           "Iceberg metadata phase=delete_manifest_scan_started selected_delete_manifests=%llu "
			           "total_delete_manifests=%llu filters=%llu",
			           new_load->manifest_indexes.size(), committed_manifest_count, filter_count);
			executor.WorkOnTasks();

			annotated_lock_guard<annotated_mutex> guard(lock);
			for (idx_t load_idx = 0; load_idx < new_load->manifest_indexes.size(); load_idx++) {
				auto manifest_idx = new_load->manifest_indexes[load_idx];
				auto &target = committed_delete_manifests[manifest_idx];
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

vector<IcebergDeleteFileReference> IcebergManifestStore::GetDeleteFiles(const vector<idx_t> &manifest_indexes) {
	vector<IcebergDeleteFileReference> result;
	auto committed_manifest_count = committed_delete_manifests.size();
	auto total_manifest_count = committed_manifest_count + transaction_delete_manifests.size();
	for (auto manifest_idx : manifest_indexes) {
		if (manifest_idx >= total_manifest_count) {
			throw InternalException("Selected delete manifest index %llu is out of bounds", manifest_idx);
		}

		if (manifest_idx < committed_manifest_count) {
			auto &manifest_list_entry = committed_delete_manifests[manifest_idx];
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
				    context.transaction_data->IsFileInvalidated(
				        {manifest_entry.data_file.file_path, manifest_entry.data_file.content_offset})) {
					continue;
				}
				result.push_back({manifest_idx, entry_idx});
			}
		} else {
			auto transaction_idx = manifest_idx - committed_manifest_count;
			auto &manifest_list_entry = transaction_delete_manifests[transaction_idx];
			auto &manifest_entries = manifest_list_entry.GetManifestEntries();
			for (idx_t entry_idx = 0; entry_idx < manifest_entries.size(); entry_idx++) {
				auto &manifest_entry = manifest_entries[entry_idx];
				if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
					continue;
				}
				if (context.transaction_data &&
				    context.transaction_data->IsFileInvalidated(
				        {manifest_entry.data_file.file_path, manifest_entry.data_file.content_offset})) {
					continue;
				}
				result.push_back({manifest_idx, entry_idx});
			}
		}
	}
	return result;
}

bool IcebergManifestStore::TryGetNextBatch(IcebergDataViewCursor &cursor) {
	if (cursor.has_current_batch || read_state.TryReadBatch(cursor)) {
		return true;
	}
	if (!data_manifest_read_state) {
		return false;
	}
	auto &scheduler = TaskScheduler::GetScheduler(context.context);
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
			if (read_state.TryReadBatch(cursor)) {
				return true;
			}
		}
		executor.WorkOnTasks();
		break;
	}
	return read_state.TryReadBatch(cursor);
}

void IcebergManifestStore::FinishScanTasks() {
	if (data_manifest_read_state) {
		data_manifest_read_state->executor.WorkOnTasks();
	}
}

const vector<IcebergManifestListEntry> &IcebergManifestStore::DataManifests() {
	return committed_data_manifests;
}

const vector<IcebergManifestListEntry> &IcebergManifestStore::DeleteManifests() {
	return committed_delete_manifests;
}

IcebergManifestStore::~IcebergManifestStore() {
	if (data_manifest_read_state) {
		try {
			data_manifest_read_state->executor.WorkOnTasks();
		} catch (...) {
			//! WorkOnTasks rethrows errors pushed by the manifest-read tasks. Destructors are implicitly
			//! noexcept (and this one can run while another exception is already unwinding), so letting the
			//! error escape calls std::terminate and aborts the whole process. Errors are still surfaced on
			//! the regular scan path (TryGetNextBatch/FinishScanTasks); here they can only be swallowed.
		}
	}
}

vector<reference<const IcebergManifestListEntry>> IcebergManifestStore::TransactionDataManifests() const {
	vector<reference<const IcebergManifestListEntry>> result;
	for (const auto &manifest : transaction_data_manifests) {
		result.push_back(manifest);
	}
	return result;
}

vector<reference<const IcebergManifestListEntry>> IcebergManifestStore::TransactionDeleteManifests() const {
	vector<reference<const IcebergManifestListEntry>> result;
	for (const auto &manifest : transaction_delete_manifests) {
		result.push_back(manifest);
	}
	return result;
}

} // namespace duckdb
