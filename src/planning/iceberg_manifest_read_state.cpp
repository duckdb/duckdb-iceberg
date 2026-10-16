#include "planning/iceberg_manifest_read_state.hpp"

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

} // namespace duckdb
