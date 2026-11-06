#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"

#include "catalog/rest/api/iceberg_add_snapshot.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"

namespace duckdb {

struct IcebergCommitState;
class IcebergSnapshotWriter;

//! What iceberg_rewrite_manifests replaces, resolved when the function is called.
struct RewriteManifestsSpec {
	//! Only DATA manifests written with this partition spec are rewritten.
	int32_t spec_id = 0;
	//! Partition field ids to cluster entries by, in priority order.
	vector<uint64_t> sort_field_ids;
	int64_t target_manifest_size_bytes = 0;
};

//! Spark rewrite_manifests output: rewritten_manifests_count, added_manifests_count.
struct RewriteManifestsResult {
	idx_t rewritten_manifests_count = 0;
	idx_t added_manifests_count = 0;
};

//! Plan from manifest-list metadata only. A table with nothing to repack yields an empty result.
RewriteManifestsResult PlanRewriteManifests(const vector<IcebergManifestListEntry> &manifests,
                                            const RewriteManifestsSpec &spec);

//! Whether a carried-forward manifest is one this rewrite replaces.
bool IsRewriteManifestsCandidate(const IcebergManifestListEntry &manifest, const RewriteManifestsSpec &spec);

//! Write the live entries of 'selected', clustered by partition, into 'output_count' new manifests.
vector<IcebergManifestListEntry> WriteRewrittenManifests(vector<IcebergManifestListEntry> &&selected,
                                                         const RewriteManifestsSpec &spec, idx_t output_count,
                                                         IcebergSnapshotWriter &writer,
                                                         IcebergCommitState &commit_state, int32_t schema_id);

//! A REPLACE snapshot that adds no files: it swaps the selected manifests for repacked ones at commit time.
struct IcebergRewriteManifestsSnapshot : public IcebergAddSnapshot {
public:
	IcebergRewriteManifestsSnapshot(const IcebergTable &table_info, RewriteManifestsSpec spec,
	                                RewriteManifestsResult planned);

public:
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;

private:
	RewriteManifestsSpec spec;
	//! The commit writes exactly the planned number of manifests, so the reported counts stay accurate.
	RewriteManifestsResult planned;
	int32_t schema_id;
};

} // namespace duckdb
