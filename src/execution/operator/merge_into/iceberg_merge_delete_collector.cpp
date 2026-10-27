#include "execution/operator/merge_into/iceberg_merge_delete_collector.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

IcebergMergeDeleteCollector::IcebergMergeDeleteCollector(idx_t action_count) : completed_actions(action_count, false) {
	if (!action_count) {
		throw InternalException("A MERGE delete collector requires at least one action");
	}
}

optional<IcebergPendingDeletes> IcebergMergeDeleteCollector::CompleteAction(idx_t action_index,
                                                                            IcebergPendingDeletes &&deletes) {
	lock_guard<mutex> guard(lock);
	if (action_index >= completed_actions.size()) {
		throw InternalException("MERGE delete action index is out of range");
	}
	if (completed_actions[action_index]) {
		throw InternalException("MERGE delete action completed more than once");
	}
	for (auto &entry : deletes.rows_by_file) {
		auto existing = pending_deletes.rows_by_file.find(entry.first);
		if (existing == pending_deletes.rows_by_file.end()) {
			pending_deletes.rows_by_file.emplace(entry.first, std::move(entry.second));
		} else {
			auto &positions = existing->second;
			// Preserve duplicates for the writer's duplicate-target check.
			positions.insert(positions.end(), entry.second.begin(), entry.second.end());
		}
	}
	completed_actions[action_index] = true;
	completed_count++;
	if (completed_count != completed_actions.size()) {
		// Never wait for another action: its pipeline may depend on this action finishing.
		return nullopt;
	}

	IcebergPendingDeletes result;
	result.rows_by_file.swap(pending_deletes.rows_by_file);
	return std::move(result);
}

bool IcebergMergeDeleteCollector::IsComplete() const {
	lock_guard<mutex> guard(lock);
	return completed_count == completed_actions.size();
}

} // namespace duckdb
