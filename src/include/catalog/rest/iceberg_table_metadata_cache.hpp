#pragma once

#include "core/metadata/iceberg_table_metadata.hpp"
#include "rest_catalog/objects/load_table_result.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

struct IcebergTable;

//! One validated LoadTable response generation. A 200 creates a new owner, even with the same ETag;
//! a 304 retains this owner. Never shared between catalog/table cache keys.
//! Retains at most one parsed payload, with no version map. Replacement/eviction drops the cache's
//! owner; in-flight requests and readers can keep the generation alive independently.
class IcebergLoadTableResponse {
public:
	explicit IcebergLoadTableResponse(rest_api_objects::LoadTableResult result) : result(std::move(result)) {
	}

	shared_ptr<const IcebergTableMetadata> GetMetadata(ClientContext &context, const IcebergTable &table) const;

	const rest_api_objects::LoadTableResult result;

private:
	mutable mutex lock;
	//! Credentials and transaction-local changes never enter this payload.
	mutable shared_ptr<const IcebergTableMetadata> metadata;
};

} // namespace duckdb
