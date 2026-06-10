#include "maintenance/table_lock_registry.hpp"

namespace duckdb {

MaintenanceTableLockGuard TableLockRegistry::TryAcquire(const MaintenanceTableKey &key, const string &action_name) {
	std::shared_ptr<MaintenanceTableLockEntry> entry;
	{
		std::lock_guard<std::mutex> map_guard(global_lock);
		auto it = table_locks.find(key);
		if (it == table_locks.end()) {
			entry = std::make_shared<MaintenanceTableLockEntry>();
			table_locks.emplace(key, entry);
		} else {
			entry = it->second;
		}
	}

	std::unique_lock<std::mutex> lock(entry->mu, std::try_to_lock);
	if (lock.owns_lock()) {
		std::lock_guard<std::mutex> hg(entry->holder_mu);
		entry->holder = action_name;
		return MaintenanceTableLockGuard(std::move(entry), std::move(lock), action_name);
	}

	string current_holder;
	{
		std::lock_guard<std::mutex> hg(entry->holder_mu);
		current_holder = entry->holder;
	}
	return MaintenanceTableLockGuard(std::move(entry), std::move(lock), std::move(current_holder));
}

TableLockRegistry &TableLockRegistry::GetInstance() {
	static TableLockRegistry instance;
	return instance;
}

} // namespace duckdb
