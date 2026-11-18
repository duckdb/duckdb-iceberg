#include "catalog/rest/iceberg_load_table_cache.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"

namespace duckdb {

LoadTableCachePublication::~LoadTableCachePublication() {
	if (cache) {
		cache->Release(*this);
	}
}

bool LoadTableCachePublication::TryPublish(shared_ptr<const IcebergLoadTableResponse> result, string etag) {
	return cache->TryPublish(*this, std::move(result), std::move(etag));
}

unique_ptr<LoadTableCachePublication> LoadTableResultCache::BeginLoad(const string &table_key) {
	auto publication = unique_ptr<LoadTableCachePublication>(new LoadTableCachePublication(table_key));
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto cached = tables.find(table_key);
	if (cached != tables.end()) {
		publication->cached_result = cached->second.load_table_result;
		publication->etag = cached->second.etag;
	}
	auto &pending = pending_loads[table_key];
	pending.latest = publication.get();
	pending.count++;
	publication->cache = this;
	return publication;
}

void LoadTableResultCache::Release(LoadTableCachePublication &publication) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto it = pending_loads.find(publication.table_key);
	D_ASSERT(it != pending_loads.end() && it->second.count > 0);
	it->second.count--;
	if (it->second.count == 0) {
		pending_loads.erase(it);
	} else if (it->second.latest.get() == &publication) {
		it->second.latest = nullptr;
	}
}

void LoadTableResultCache::InvalidateLoads(const string &table_key) {
	auto it = pending_loads.find(table_key);
	if (it != pending_loads.end()) {
		it->second.latest = nullptr;
	}
}

bool LoadTableResultCache::TryPublish(LoadTableCachePublication &publication,
                                      shared_ptr<const IcebergLoadTableResponse> result, string etag) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto it = pending_loads.find(publication.table_key);
	if (it == pending_loads.end() || it->second.latest.get() != &publication) {
		return false;
	}
	Store(publication.table_key, std::move(result), std::move(etag));
	it->second.latest = nullptr;
	return true;
}

void LoadTableResultCache::SetOrOverwrite(const string &table_key, shared_ptr<const IcebergLoadTableResponse> result) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	InvalidateLoads(table_key);
	Store(table_key, std::move(result), "");
}

void LoadTableResultCache::Store(const string &table_key, shared_ptr<const IcebergLoadTableResponse> result,
                                 string etag) {
	// With staleness disabled, retain the payload for ETag revalidation and identity-based invalidation,
	// but require a catalog request before reusing it.
	system_clock::time_point expires_at;
	if (attach_options.max_table_staleness_micros.IsValid()) {
		expires_at =
		    system_clock::now() + std::chrono::microseconds(attach_options.max_table_staleness_micros.GetIndex());
	} else {
		expires_at = system_clock::time_point::min();
	}
	auto epoch_micros = timestamp_t(duration_cast<microseconds>(expires_at.time_since_epoch()).count());
	auto expire_timestamp_ms = timestamp_ms_t(Timestamp::GetEpochMs(epoch_micros));
	tables.erase(table_key);
	tables.emplace(table_key, MetadataCacheValue(expire_timestamp_ms, std::move(result), std::move(etag)));
}

void LoadTableResultCache::EvictIfCurrent(const IcebergTable &table) {
	EvictIfCurrent(table.GetTableKey(), table.initialization_source.get());
}

void LoadTableResultCache::EvictIfCurrent(const string &table_key,
                                          optional_ptr<const IcebergLoadedTable> initialization_source) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	// Even without a matching cached payload, a pre-write fetch must not repopulate the cache.
	InvalidateLoads(table_key);
	auto it = tables.find(table_key);
	if (it == tables.end()) {
		return;
	}
	const IcebergLoadedTable *cached = it->second.load_table_result.get();
	if (cached != initialization_source.get()) {
		return;
	}
	tables.erase(it);
}

} // namespace duckdb
