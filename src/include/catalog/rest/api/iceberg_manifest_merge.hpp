#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/copy_function.hpp"

#include "core/metadata/manifest/iceberg_manifest_list.hpp"

namespace duckdb {

struct IcebergTableMetadata;
struct IcebergCommitState;

//! Origin of a manifest fed into the merge step. The min-count guard only applies to bins that
//! contain at least one NEW_THIS_TRANSACTION manifest, so we must track this explicitly rather
//! than rely on positional assumptions (see design doc B18).
enum class ManifestSource : uint8_t { NEW_THIS_TRANSACTION, CARRIED_OVER };

//! A manifest handed to the merge step, tagged with its origin. The entry's `manifest_entries`
//! may be empty (unloaded): the merge decision uses only manifest-file-level metadata.
struct MergeInputManifest {
	IcebergManifestListEntry entry;
	ManifestSource source;
};

//! Resolved `commit.manifest.*` / `commit.manifest-merge.enabled` table properties.
struct ManifestMergeConfig {
	bool enabled;
	idx_t min_count_to_merge;
	int64_t target_size_bytes;

	static ManifestMergeConfig FromTableMetadata(const IcebergTableMetadata &metadata);
};

//! First-fit bin-packing of weights against `target_weight`, packing from the tail to mirror
//! Java/PyIceberg's `pack_end` (reverse -> first-fit -> reverse). Returns, for each bin, the list
//! of input indices it contains, in original order. Pure logic, unit-testable, no IO.
vector<vector<idx_t>> BinPackManifests(const vector<int64_t> &weights, int64_t target_weight);

//! Read the manifest_entries of a manifest from its Avro file, reusing the vectorized manifest
//! reader. Returns the list entry with `manifest_entries` populated. Shared by the delete-rewrite
//! path and the merge path so both load entries identically.
IcebergManifestListEntry ScanManifestEntries(const IcebergManifestListEntry &list_entry,
                                             IcebergCommitState &commit_state, int32_t schema_id);

//! Decide whether a bin should be physically merged into a single manifest.
//!  - a single-manifest bin is never merged
//!  - a bin that contains a new-this-transaction manifest is merged only once it reaches
//!    `min_count_to_merge` (avoids repeatedly rewriting small new manifests)
//!  - any other multi-manifest bin is merged
bool ShouldMergeBin(const vector<MergeInputManifest> &input, const vector<idx_t> &bin, idx_t min_count_to_merge);

//! Merge a set of manifests of a single content type (DATA or DELETE; the two are never mixed).
//! Bins selected for merge have their entries read and rewritten into a single new manifest;
//! everything else is passed through unchanged. Returns the resulting manifest-list entries.
//!
//! Sequence-number rules (design doc A1): entries pulled from already-committed manifests keep
//! their original (historical) sequence numbers and are written EXISTING; entries that are new in
//! this transaction keep status ADDED with inherited (NULL) sequence numbers and are never demoted.
//! `snapshot_id` is the id of the snapshot being created; it is used to decide which DELETED entries
//! to retain (only deletes made by this snapshot) -- mirroring Java's ManifestMergeManager.
vector<IcebergManifestListEntry> MergeManifests(vector<MergeInputManifest> &&input,
                                                IcebergManifestContentType content,
                                                const ManifestMergeConfig &config, CopyFunction &avro_copy,
                                                DatabaseInstance &db, IcebergCommitState &commit_state,
                                                int32_t schema_id, int64_t snapshot_id);

} // namespace duckdb
