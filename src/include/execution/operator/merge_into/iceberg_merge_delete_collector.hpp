#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional.hpp"
#include "execution/operator/iceberg_pending_deletes.hpp"

namespace duckdb {

//! Runtime state for the delete-producing actions of one MERGE execution.
class IcebergMergeDeleteCollector {
public:
	explicit IcebergMergeDeleteCollector(idx_t action_count);

	//! Every action, including an empty one, contributes exactly once after its sink finishes.
	//! Only the last contributor receives a batch. An empty batch still signals completion.
	optional<IcebergPendingDeletes> CompleteAction(idx_t action_index, IcebergPendingDeletes &&deletes);
	bool IsComplete() const;
	idx_t ActionCount() const {
		return completed_actions.size();
	}

private:
	mutable mutex lock;
	vector<bool> completed_actions;
	idx_t completed_count = 0;
	IcebergPendingDeletes pending_deletes;
};

} // namespace duckdb
