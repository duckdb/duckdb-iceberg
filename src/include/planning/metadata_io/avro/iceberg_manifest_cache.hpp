#pragma once

#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "planning/deletes/iceberg_delete_candidate_index.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

class IcebergManifestFileScanInfo;

//! Immutable file content, independent of manifest-list inheritance and scan state.
class IcebergParsedManifest : public ObjectCacheEntry {
public:
	IcebergParsedManifest(ClientContext &context, IcebergManifestListEntry &manifest,
	                      const IcebergTableMetadata &metadata, idx_t key_size);

	static string ObjectType() {
		return "iceberg_parsed_manifest";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return estimated_memory;
	}
	const vector<IcebergManifestEntry> &Entries() const {
		return *entries;
	}
	const shared_ptr<const vector<IcebergManifestEntry>> &SharedEntries() const {
		return entries;
	}
	const shared_ptr<const IcebergDeleteCandidateIndex> &DeleteIndex() const {
		return delete_index;
	}
	IcebergManifestMetadata Validate(const IcebergManifestFileScanInfo &scan,
	                                 const IcebergManifestListEntry &manifest) const;

private:
	const shared_ptr<const vector<IcebergManifestEntry>> entries;
	const InsertionOrderPreservingMap<Value> header;
	shared_ptr<const IcebergDeleteCandidateIndex> delete_index;
	idx_t estimated_memory;
};

//! Owned allocations in entries, including unused vector capacity.
idx_t EstimateManifestEntries(const vector<IcebergManifestEntry> &entries);
//! Length-delimited identity includes the resolved path and actual decoding contract.
string ManifestCacheKey(const IcebergManifestFileScanInfo &scan, idx_t manifest_idx);

} // namespace duckdb
