#pragma once

#include "core/metadata/manifest/iceberg_manifest.hpp"

namespace duckdb {

//! Immutable, snapshot-independent content of one completed delete manifest.
//! Entry indexes refer only to entries owned here. Inheritance, view predicates,
//! provider associations and transaction invalidations belong to the scan.
class IcebergDeleteCandidateIndex {
public:
	IcebergDeleteCandidateIndex(ClientContext &context, vector<IcebergManifestEntry> entries,
	                            const IcebergPartitionSpec &spec);
	~IcebergDeleteCandidateIndex();

	//! At most three borrowed buckets: original referenced path, partition and fallback.
	//! Keep the index alive while using these references.
	vector<reference<const vector<idx_t>>> Lookup(const string &original_data_path, int32_t data_spec_id,
	                                              const partition_value_map_t &partition_values) const;
	const vector<IcebergManifestEntry> &Entries() const {
		return entries;
	}

private:
	struct PartitionIndex;
	bool TryPartitionKey(const partition_value_map_t &values, vector<Value> &key) const;

	vector<IcebergManifestEntry> entries;
	int32_t spec_id;
	vector<uint64_t> partition_fields;
	vector<LogicalType> partition_types;
	unordered_map<string, vector<idx_t>> by_referenced_path;
	//! Frozen DuckDB groups own typed keys and stable rows for the per-group entry lists.
	//! Its buffer-managed storage must be released before the database is destroyed.
	unique_ptr<PartitionIndex> by_partition;
	//! Also used for an unrepresentable lookup key. Never omit a potential match.
	vector<idx_t> unreferenced;
	vector<idx_t> fallback;
};

//! The manifest-list index is scan-local; the owned index above is reusable.
struct IcebergDeleteManifestHandle {
	idx_t manifest_idx;
	shared_ptr<const IcebergDeleteCandidateIndex> index;
};

} // namespace duckdb
