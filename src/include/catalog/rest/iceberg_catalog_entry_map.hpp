#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

//! Foreground-owned entries with a one-time handoff from background discovery.
//! The owning catalog set serializes GetEntries/GetPrefetch with its entry lock.
template <class ENTRY>
class IcebergCatalogEntryMap {
public:
	using Entries = unordered_map<string, shared_ptr<ENTRY>>;

	class Prefetch {
	public:
		//! Publish a privately built map, never individual changes to the live catalog.
		void Publish(Entries result) {
			lock_guard<mutex> guard(lock);
			entries = std::move(result);
		}

	private:
		friend class IcebergCatalogEntryMap;

		Entries Take() {
			lock_guard<mutex> guard(lock);
			return std::move(entries);
		}

		mutex lock;
		Entries entries;
	};

	IcebergCatalogEntryMap() : prefetch(make_shared_ptr<Prefetch>()) {
	}
	IcebergCatalogEntryMap(const IcebergCatalogEntryMap &) = delete;
	IcebergCatalogEntryMap &operator=(const IcebergCatalogEntryMap &) = delete;

	Entries &GetEntries() {
		if (prefetch) {
			// Claim the handoff even if the request has not finished. Its publisher then owns only an
			// unreachable snapshot, so a late response cannot undo a foreground lookup or mutation.
			auto initial = std::move(prefetch);
			entries = initial->Take();
		}
		return entries;
	}

	//! Capture before exposing the catalog/schema to foreground callers.
	shared_ptr<Prefetch> GetPrefetch() const {
		return prefetch;
	}

private:
	Entries entries;
	shared_ptr<Prefetch> prefetch;
};

} // namespace duckdb
