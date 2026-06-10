#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"

#include <memory>
#include <mutex>

namespace duckdb {

//! Three-part identifier for an Iceberg table inside a catalog, used as the
//! key for table-level maintenance mutexes.
//!
//! Components are lower-cased at parse time (simple identifiers only) so the
//! lock key is case-insensitive, matching DuckDB's identifier resolution.
struct MaintenanceTableKey {
	string catalog;
	string schema;
	string table;

	bool operator==(const MaintenanceTableKey &other) const {
		return catalog == other.catalog && schema == other.schema && table == other.table;
	}
};

struct MaintenanceTableKeyHash {
	size_t operator()(const MaintenanceTableKey &k) const noexcept {
		auto h1 = std::hash<string>()(k.catalog);
		auto h2 = std::hash<string>()(k.schema);
		auto h3 = std::hash<string>()(k.table);
		return h1 ^ (h2 << 1) ^ (h3 << 2);
	}
};

//! Per-table mutex plus the action name currently holding it. `holder` is
//! protected by `holder_mu` (a tiny separate mutex) so contended callers can
//! read the current holder without contending on `mu` itself — they already
//! failed `try_lock(mu)`, we don't make them wait again.
struct MaintenanceTableLockEntry {
	std::mutex mu;
	std::mutex holder_mu;
	string holder;
};

//! RAII lock guard returned by `TableLockRegistry::TryAcquire`. Holding an
//! instance with `Owns() == true` means the caller owns the table-level
//! maintenance lock; the lock is released when the guard goes out of scope.
//!
//! On contention, `Owns()` is false and `HolderName()` reports which action
//! currently owns the table. The caller decides how to handle contention
//! (e.g. throw CatalogException).
class MaintenanceTableLockGuard {
public:
	MaintenanceTableLockGuard() = default;
	MaintenanceTableLockGuard(MaintenanceTableLockGuard &&) noexcept = default;
	MaintenanceTableLockGuard &operator=(MaintenanceTableLockGuard &&) noexcept = default;
	MaintenanceTableLockGuard(const MaintenanceTableLockGuard &) = delete;
	MaintenanceTableLockGuard &operator=(const MaintenanceTableLockGuard &) = delete;

	~MaintenanceTableLockGuard() {
		if (lock.owns_lock() && entry) {
			std::lock_guard<std::mutex> hg(entry->holder_mu);
			entry->holder.clear();
		}
	}

	bool Owns() const {
		return lock.owns_lock();
	}

	//! On contention: the action name currently owning the lock (may be empty
	//! if the lock was released between our `try_lock` and the holder read).
	//! On success: the action name we wrote into the entry — usually unused.
	const string &HolderName() const {
		return holder;
	}

private:
	friend class TableLockRegistry;
	MaintenanceTableLockGuard(std::shared_ptr<MaintenanceTableLockEntry> entry,
	                          std::unique_lock<std::mutex> lock, string holder)
	    : entry(std::move(entry)), lock(std::move(lock)), holder(std::move(holder)) {
	}

	std::shared_ptr<MaintenanceTableLockEntry> entry;
	std::unique_lock<std::mutex> lock;
	string holder;
};

//! Process-wide registry of per-table maintenance mutexes.
//!
//! `try_lock` semantics ensure maintenance calls never block on each other:
//! a contended caller gets a non-owning guard and can throw or return
//! immediately.
class TableLockRegistry {
public:
	//! Try to acquire the lock for `key` on behalf of `action_name`. Returns a
	//! guard whose `Owns()` is true on success and false on contention. Never
	//! blocks. On contention `guard.HolderName()` reports the action that
	//! currently owns the lock.
	MaintenanceTableLockGuard TryAcquire(const MaintenanceTableKey &key, const string &action_name);

	//! Process-wide singleton. The registry is naturally cleaned up at
	//! process exit; in-flight locks are released as their guards die.
	static TableLockRegistry &GetInstance();

private:
	std::mutex global_lock;
	unordered_map<MaintenanceTableKey, std::shared_ptr<MaintenanceTableLockEntry>, MaintenanceTableKeyHash> table_locks;
};

} // namespace duckdb
