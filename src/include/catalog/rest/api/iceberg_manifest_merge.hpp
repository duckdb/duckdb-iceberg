#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/copy_function.hpp"

#include "core/metadata/manifest/iceberg_manifest_list.hpp"

namespace duckdb {

struct IcebergTableMetadata;
struct IcebergCommitState;

//! Resolved `commit.manifest.*` / `commit.manifest-merge.enabled` table properties.
struct IcebergManifestMergeConfig {
	bool enabled;
	idx_t min_count_to_merge;
	int64_t target_size_bytes;

	static IcebergManifestMergeConfig FromTableMetadata(const IcebergTableMetadata &metadata, ClientContext &context);
};

struct IcebergManifestMerge {
	//! First-fit bin-packing of weights against `target_weight`, packing from the tail to mirror
	//! Java/PyIceberg's `pack_end` (reverse -> first-fit -> reverse). Returns, for each bin, the list
	//! of input indices it contains, in original order. Pure logic, unit-testable, no IO.
	static vector<vector<idx_t>> BinPackManifests(const vector<int64_t> &weights, int64_t target_weight);

	//! Read the manifest_entries of a manifest from its Avro file, reusing the vectorized manifest
	//! reader. Returns the list entry with `manifest_entries` populated. Shared by the delete-rewrite
	//! path and the merge path so both load entries identically.
	static IcebergManifestListEntry ScanManifestEntries(const IcebergManifestListEntry &list_entry,
	                                                    IcebergCommitState &commit_state, int32_t schema_id);

	//! Build and persist a replacement manifest from already-materialized entries. The metadata-driven
	//! manifest creation is shared by merge-append and delete-driven rewrites; callers can override
	//! file-level lineage/sequence metadata when the physical rewrite must preserve historical values.
	static IcebergManifestListEntry WriteReplacementManifest(const IcebergManifestMetadata &manifest_metadata,
	                                                         vector<IcebergManifestEntry> &&manifest_entries,
	                                                         CopyFunction &avro_copy, DatabaseInstance &db,
	                                                         IcebergCommitState &commit_state,
	                                                         optional<sequence_number_t> first_row_id = nullopt,
	                                                         optional<sequence_number_t> min_sequence_number = nullopt);

	//! Decide whether a bin should be physically merged into a single manifest:
	//!  - a single-manifest bin is never merged;
	//!  - a bin is merged only once it holds at least `min_count_to_merge` manifests (Apache Iceberg's
	//!    ManifestMergeManager semantics), so under-filled bins are left alone until enough small
	//!    manifests accumulate.
	static bool ShouldMergeBin(const vector<idx_t> &bin, idx_t min_count_to_merge);

	//! Merge a set of already-committed manifests of a single content type (DATA or DELETE; the two
	//! are never mixed). Manifests are grouped by (schema id, partition spec id) -- each manifest's
	//! schema id is resolved by opening the file -- and only manifests sharing both are candidates to
	//! merge. Bins selected for merge are rewritten into a single new manifest; everything else is
	//! passed through unchanged.
	static vector<IcebergManifestListEntry> MergeManifests(vector<IcebergManifestListEntry> &&input,
	                                                       IcebergManifestContentType content,
	                                                       const IcebergManifestMergeConfig &config,
	                                                       CopyFunction &avro_copy, DatabaseInstance &db,
	                                                       IcebergCommitState &commit_state, int32_t current_schema_id);

	//! Repack the loaded committed manifest set into fewer manifests, in place.
	static void MergeManifestList(vector<IcebergManifestListEntry> &manifests, int32_t current_schema_id,
	                              CopyFunction &avro_copy, DatabaseInstance &db, IcebergCommitState &commit_state);
};

} // namespace duckdb
