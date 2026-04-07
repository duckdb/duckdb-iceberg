#pragma once

#include "duckdb.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

//! A batch of scanned/cached IcebergManifestEntry items to read
struct ManifestReadBatch {
public:
	ManifestReadBatch() {
	}
	ManifestReadBatch(idx_t manifest_list_entry_idx, idx_t start_index, idx_t end_index)
	    : manifest_list_entry_idx(manifest_list_entry_idx), start_index(start_index), end_index(end_index) {
	}

public:
	idx_t manifest_list_entry_idx;
	idx_t start_index;
	idx_t end_index;
};

struct ManifestEntryReadState {
public:
	void PushBatch(ManifestReadBatch &&batch);
	bool HasCurrentBatch() const;
	optional_ptr<ManifestReadBatch> GetCurrentBatch();
	void FinishBatch();

private:
	bool has_batch = false;
	ManifestReadBatch current_batch;

private:
	//! Lock guarding the batches against concurrent access
	mutex lock;
	queue<ManifestReadBatch> batches;
};

} // namespace duckdb
