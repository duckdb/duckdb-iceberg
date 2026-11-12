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
	    : context(context), executor(context), scan(std::move(scan)), list_entries(list_entries) {
	}

	ClientContext &context;
	TaskExecutor executor;
	unique_ptr<AvroScan> scan;
	vector<IcebergManifestListEntry> &list_entries;
};

struct IcebergDeleteManifestLoadState {
	mutex lock;
	std::condition_variable cv;
	bool complete = false;
	ErrorData error;
	vector<idx_t> manifest_indexes;
	vector<reference<const IcebergManifestListEntry>> sources;
	vector<IcebergManifestListEntry> manifests;
	shared_ptr<IcebergManifestScanningState> scan_state;
};

namespace {

class ManifestReadTask : public BaseExecutorTask {
public:
	ManifestReadTask(IcebergManifestScanningState &state) : BaseExecutorTask(state.executor), reader(*state.scan) {
	}

	//! One manifest read per step. The wrapper loops this while draining and yields between steps.
	TaskExecutionResult ExecuteTaskStep() override {
		if (!reader.Finished()) {
			reader.Read();
			return TaskExecutionResult::TASK_NOT_FINISHED;
		}
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return "IcebergManifestReadTask";
	}

private:
	manifest_file::ManifestReader reader;
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
				IcebergManifestList::LoadManifestFiles(snapshot_info, metadata, context.context, manifest_list_entries,
				                                       context.file_secret);
			} else {
				auto manifest_list_full_path = context.options.allow_moved_paths
				                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
				                                   : snapshot.manifest_list;
				auto scan =
				    AvroScan::ScanManifestList(snapshot_info, metadata, context.context, manifest_list_full_path,
				                               manifest_list_entries, context.file_secret);
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
		idx_t max_reserved_entries = DEFAULT_MAX_RESERVED_MANIFEST_ENTRIES;
		Value max_reserved_setting;
		if (context.context.TryGetCurrentSetting(MAX_RESERVED_MANIFEST_ENTRIES_CONFIG_VARIABLE, max_reserved_setting) &&
		    !max_reserved_setting.IsNull()) {
			max_reserved_entries = max_reserved_setting.GetValue<uint64_t>();
		}
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
			//! Every entry takes at least one byte of the manifest: counts beyond its length cannot be reserved
			//! exactly, so load the manifest eagerly like one without counts
			auto manifest_length = list_entry.GetFile().manifest_length;
			if (manifest_length > 0 && reserve_size > static_cast<idx_t>(manifest_length)) {
				manifests_to_eagerly_load.push_back(manifest_idx);
				continue;
			}
			if (reserve_size > max_reserved_entries) {
				throw InvalidConfigurationException(
				    "Manifest '%s' declares %llu entries in the manifest list, which exceeds the current limit of %llu "
				    "entries reserved per manifest. Raise the limit with SET %s (the maximum is %llu)",
				    list_entry.GetFile().manifest_path, reserve_size, max_reserved_entries,
				    MAX_RESERVED_MANIFEST_ENTRIES_CONFIG_VARIABLE, MAX_MAX_RESERVED_MANIFEST_ENTRIES);
			}
			list_entry.GetOrCreateManifestEntries().reserve(reserve_size);
		}

		if (!manifests_to_eagerly_load.empty()) {
			auto scan = AvroScan::ScanManifest(context.snapshot, data_manifests, context.options, context.fs,
			                                   context.path, context.metadata, context.context, nullptr,
			                                   manifests_to_eagerly_load, context.file_secret);
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
		auto count = committed_delete_manifests.size() + transaction_delete_manifests.size();
		delete_manifest_loads.resize(count);
		delete_indexes.resize(count);
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
		                                        selected_committed_manifests, context.file_secret);
		data_manifest_read_state =
		    make_uniq<IcebergManifestScanningState>(context.context, std::move(data_scan), committed_data_manifests);

		auto &executor = data_manifest_read_state->executor;
		auto &scheduler = TaskScheduler::GetScheduler(context.context);
		auto num_threads = MinValue<idx_t>(scheduler.NumberOfThreads(), selected_committed_manifests.size());
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
	unordered_set<idx_t> pending_indexes;
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
			auto &manifest = manifest_idx < committed_manifest_count
			                     ? committed_delete_manifests[manifest_idx]
			                     : transaction_delete_manifests[manifest_idx - committed_manifest_count];
			if (delete_indexes[manifest_idx]) {
				//! Scan for this manifest is already completed
				continue;
			}
			auto &load = delete_manifest_loads[manifest_idx];
			if (!load) {
				if (!pending_indexes.insert(manifest_idx).second) {
					continue;
				}
				//! No load for this manifest exists yet
				if (!new_load) {
					//! We haven't created a load yet, initialize it here
					new_load = make_shared_ptr<IcebergDeleteManifestLoadState>();
					required_loads.push_back(new_load);
				}
				new_load->manifest_indexes.push_back(manifest_idx);
				new_load->sources.push_back(manifest);
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
		// Register only after collecting all source references. An exception here
		// must not leave a visible load without an owner.
		if (new_load) {
			for (auto manifest_idx : new_load->manifest_indexes) {
				delete_manifest_loads[manifest_idx] = new_load;
			}
		}
	}

	if (new_load) {
		//! At least one of the manifests we need aren't referenced yet, need to start a scan for it/them
		ErrorData load_error;
		try {
			// Source descriptors and pending entries are frozen after LoadManifestList.
			// Copy pending content outside the planning lock, within the failure boundary.
			for (auto &source : new_load->sources) {
				new_load->manifests.push_back(source.get());
			}
			vector<idx_t> to_read;
			for (idx_t i = 0; i < new_load->manifests.size(); i++) {
				if (!new_load->manifests[i].HasManifestEntries()) {
					to_read.push_back(i);
				}
			}
			if (!to_read.empty()) {
				auto scan = AvroScan::ScanManifest(context.snapshot, new_load->manifests, context.options, context.fs,
				                                   context.path, context.metadata, context.context, nullptr, to_read,
				                                   context.file_secret);
				new_load->scan_state = make_shared_ptr<IcebergManifestScanningState>(context.context, std::move(scan),
				                                                                     new_load->manifests);
				auto &executor = new_load->scan_state->executor;
				auto &scheduler = TaskScheduler::GetScheduler(context.context);
				auto num_threads = MinValue<idx_t>(scheduler.NumberOfThreads(), to_read.size());
				for (idx_t i = 0; i < num_threads; i++) {
					executor.ScheduleTask(make_uniq<ManifestReadTask>(*new_load->scan_state));
				}
				DUCKDB_LOG(context.context, IcebergLogType,
				           "Iceberg metadata phase=delete_manifest_scan_started selected_delete_manifests=%llu "
				           "total_delete_manifests=%llu filters=%llu",
				           to_read.size(), committed_manifest_count, filter_count);
				executor.WorkOnTasks();
			}
			// Build the entire load before publication. On failure all overlapping waiters
			// observe the same error, and no partial set of indexes becomes visible.
			vector<shared_ptr<const IcebergDeleteCandidateIndex>> indexes;
			for (auto &manifest : new_load->manifests) {
				auto &spec = context.metadata.partition_specs.at(manifest.GetManifest().partition_spec_id);
				indexes.push_back(make_shared_ptr<IcebergDeleteCandidateIndex>(
				    context.context, std::move(manifest.GetManifestEntries()), spec));
			}
			{
				annotated_lock_guard<annotated_mutex> guard(lock);
				for (idx_t i = 0; i < indexes.size(); i++) {
					delete_indexes[new_load->manifest_indexes[i]] = std::move(indexes[i]);
				}
			}
			DUCKDB_LOG(context.context, IcebergLogType,
			           "Iceberg metadata phase=delete_candidate_index scope=manifest_load delete_index_builds=%llu",
			           new_load->manifest_indexes.size());
		} catch (std::exception &ex) {
			load_error = ErrorData(ex);
		} catch (...) { // LCOV_EXCL_START
			load_error = ErrorData("Unknown exception while reading Iceberg delete manifests");
		} // LCOV_EXCL_STOP

		// Scheduling can fail after submitting tasks. Drain before any load storage
		// can be released, preserving the original error for every waiter.
		if (load_error.HasError() && new_load->scan_state) {
			try {
				new_load->scan_state->executor.WorkOnTasks();
			} catch (...) {
			}
		}
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

vector<IcebergDeleteManifestHandle> IcebergManifestStore::GetDeleteIndexes(const vector<idx_t> &manifest_indexes) {
	vector<IcebergDeleteManifestHandle> result;
	for (auto manifest_idx : manifest_indexes) {
		if (manifest_idx >= delete_indexes.size() || !delete_indexes[manifest_idx]) {
			throw InternalException("Selected delete manifest %llu has no completed index", manifest_idx);
		}
		result.push_back({manifest_idx, delete_indexes[manifest_idx]});
	}
	return result;
}

bool IcebergManifestStore::DeleteFileIsInvalidated(const IcebergFileIdentity &file) const {
	return context.transaction_data && context.transaction_data->IsFileInvalidated(file);
}

bool IcebergManifestStore::TryGetNextBatch(IcebergDataViewCursor &cursor) {
	if (cursor.has_current_batch || read_state.TryReadBatch(cursor)) {
		return true;
	}
	if (!data_manifest_read_state) {
		return false;
	}
	auto &scheduler = TaskScheduler::GetScheduler(context.context);
	auto &executor = data_manifest_read_state->executor;
	shared_ptr<Task> task_to_execute;
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
	//! The executor tracks completion and errors; a finished task can be destroyed after the scan state.
	executor.WorkOnTasks();
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
