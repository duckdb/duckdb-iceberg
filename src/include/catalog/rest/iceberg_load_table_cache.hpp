//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/rest/iceberg_load_table_cache.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/thread_annotation.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/unordered_map.hpp"

#include "catalog/rest/iceberg_table_metadata_cache.hpp"
#include "common/iceberg_utils.hpp"
#include "iceberg_attach.hpp"

namespace duckdb {

struct IcebergTable;

//! The LoadTable responses of a REST catalog: the MAX_TABLE_STALENESS cache, and the ETags that revalidate them.
//! It belongs to the REST backend.
class MetadataCacheValue {
public:
	MetadataCacheValue(timestamp_ms_t expire_timestamp_ms, shared_ptr<const IcebergLoadTableResponse> load_table_result,
	                   string etag)
	    : expire_timestamp_ms(expire_timestamp_ms), load_table_result(std::move(load_table_result)),
	      etag(std::move(etag)) {
	}

public:
	//! The timestamp until when this entry is valid
	timestamp_ms_t expire_timestamp_ms;
	//! The payload of the cache entry
	shared_ptr<const IcebergLoadTableResponse> load_table_result;
	string etag;
};

class LoadTableResultCache;

//! Caller-owned state controlling whether a fetched result can enter the cache.
//! The cache must outlive this state, which is kept separate from worker requests.
class LoadTableCachePublication {
public:
	~LoadTableCachePublication();
	bool TryPublish(shared_ptr<const IcebergLoadTableResponse> result, string etag);

	//! An expired response may still be revalidated with its ETag.
	shared_ptr<const IcebergLoadTableResponse> cached_result;
	string etag;

private:
	friend class LoadTableResultCache;
	explicit LoadTableCachePublication(string table_key) : table_key(std::move(table_key)) {
	}
	LoadTableCachePublication(const LoadTableCachePublication &) = delete;
	LoadTableCachePublication &operator=(const LoadTableCachePublication &) = delete;

	optional_ptr<LoadTableResultCache> cache;
	string table_key;
};

class LoadTableResultCache {
public:
	LoadTableResultCache(IcebergAttachOptions &attach_options) : attach_options(attach_options) {
	}

public:
	bool Get(ClientContext &context, const string &table_key,
	         const std::function<void(shared_ptr<const IcebergLoadTableResponse>)> &callback,
	         bool validate_cache = true) {
		shared_ptr<const IcebergLoadTableResponse> response;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			auto it = tables.find(table_key);
			if (it == tables.end()) {
				return false;
			}
			auto transaction_start_ms = IcebergUtils::GetTransactionStartTimeMS(context);
			if (validate_cache && transaction_start_ms > it->second.expire_timestamp_ms) {
				return false;
			}
			response = it->second.load_table_result;
		}
		// Metadata parsing (including authoritative file I/O) never holds the catalog-wide lock.
		callback(std::move(response));
		return true;
	}
	//! Create publication state before fetching. A later fetch supersedes earlier loads for the same key.
	unique_ptr<LoadTableCachePublication> BeginLoad(const string &table_key);
	//! Authoritative publication (e.g. staged creation) also invalidates outstanding fetches.
	void SetOrOverwrite(const string &table_key, shared_ptr<const IcebergLoadTableResponse> load_table_result);

	//! Evict only if the table was initialized from the result that is still cached for its key.
	void EvictIfCurrent(const IcebergTable &table);
	void EvictIfCurrent(const string &table_key, optional_ptr<const IcebergLoadedTable> initialization_source);

private:
	friend class LoadTableCachePublication;
	struct PendingLoads {
		optional_ptr<LoadTableCachePublication> latest;
		idx_t count = 0;
	};
	bool TryPublish(LoadTableCachePublication &publication, shared_ptr<const IcebergLoadTableResponse> result,
	                string etag);
	void Release(LoadTableCachePublication &publication);
	void InvalidateLoads(const string &table_key) DUCKDB_REQUIRES(lock);
	void Store(const string &table_key, shared_ptr<const IcebergLoadTableResponse> result, string etag)
	    DUCKDB_REQUIRES(lock);

	IcebergAttachOptions &attach_options;
	annotated_mutex lock;
	unordered_map<string, MetadataCacheValue> tables DUCKDB_GUARDED_BY(lock);
	unordered_map<string, PendingLoads> pending_loads DUCKDB_GUARDED_BY(lock);
};

} // namespace duckdb
