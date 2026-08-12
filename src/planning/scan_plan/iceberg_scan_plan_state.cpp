#include "planning/scan_plan/iceberg_scan_plan_state.hpp"

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

bool ManifestEntryReadState::TryReadBatch(IcebergDataViewCursor &cursor) const {
	if (!GetBatch(cursor.next_batch_idx, cursor.current_batch)) {
		return false;
	}
	cursor.next_batch_idx++;
	cursor.current_batch_offset = cursor.current_batch.start_index;
	cursor.has_current_batch = true;
	return true;
}

IcebergScanPlanState::IcebergScanPlanState(ClientContext &context_p, shared_ptr<IcebergScanInfo> scan_info_p,
                                           string path_p, const IcebergOptions &options_p)
    : context(context_p), fs(FileSystem::GetFileSystem(context)), scan_info(std::move(scan_info_p)),
      path(std::move(path_p)), options(options_p) {
}

IcebergScanPlanState::~IcebergScanPlanState() {
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

} // namespace duckdb
