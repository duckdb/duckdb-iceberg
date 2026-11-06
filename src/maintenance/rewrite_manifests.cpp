#include "maintenance/rewrite_manifests.hpp"

#include "duckdb/common/numeric_utils.hpp"

#include "catalog/rest/api/iceberg_manifest_merge.hpp"
#include "catalog/rest/api/iceberg_table_update.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/snapshot/iceberg_snapshot_writer.hpp"

#include <algorithm>

namespace duckdb {

namespace {

optional_ptr<const Value> FindPartitionValue(const IcebergDataFile &data_file, uint64_t field_id) {
	for (auto &partition : data_file.partition_info) {
		if (partition.field_id == field_id) {
			return &partition.value;
		}
	}
	return nullptr;
}

//! Orders entries by the requested partition fields, NULLs first, like Spark's range repartitioning.
bool PartitionLessThan(const IcebergManifestEntry &a, const IcebergManifestEntry &b,
                       const vector<uint64_t> &sort_field_ids) {
	for (auto field_id : sort_field_ids) {
		auto a_value = FindPartitionValue(a.data_file, field_id);
		auto b_value = FindPartitionValue(b.data_file, field_id);
		const bool a_null = !a_value || a_value->IsNull();
		const bool b_null = !b_value || b_value->IsNull();
		if (a_null || b_null) {
			if (a_null != b_null) {
				return a_null;
			}
			continue;
		}
		if (*a_value < *b_value) {
			return true;
		}
		if (*b_value < *a_value) {
			return false;
		}
	}
	return false;
}

} // namespace

bool IsRewriteManifestsCandidate(const IcebergManifestListEntry &manifest, const RewriteManifestsSpec &spec) {
	auto &file = manifest.GetFile();
	return file.content == IcebergManifestContentType::DATA && file.partition_spec_id == spec.spec_id;
}

RewriteManifestsResult PlanRewriteManifests(const vector<IcebergManifestListEntry> &manifests,
                                            const RewriteManifestsSpec &spec) {
	RewriteManifestsResult result;
	int64_t total_bytes = 0;
	int64_t live_files = 0;
	for (auto &manifest : manifests) {
		if (!IsRewriteManifestsCandidate(manifest, spec)) {
			continue;
		}
		auto &file = manifest.GetFile();
		result.rewritten_manifests_count++;
		total_bytes += file.manifest_length;
		if (!file.counts || !file.counts->FilesComplete()) {
			throw InvalidConfigurationException("iceberg_rewrite_manifests: manifest '%s' has no file counts",
			                                    file.manifest_path);
		}
		live_files += NumericCast<int64_t>(*file.counts->added_files_count + *file.counts->existing_files_count);
	}
	if (result.rewritten_manifests_count == 0) {
		return result;
	}

	//! Spark RewriteManifestsSparkAction: ceil(total size / target size), and a lone manifest that would be
	//! rewritten into a lone manifest is left alone.
	auto target_manifests = (total_bytes + spec.target_manifest_size_bytes - 1) / spec.target_manifest_size_bytes;
	target_manifests = MaxValue<int64_t>(target_manifests, 1);
	if (result.rewritten_manifests_count == 1 && target_manifests == 1) {
		return RewriteManifestsResult();
	}
	//! Manifests holding only tombstones are replaced by nothing.
	result.added_manifests_count = NumericCast<idx_t>(MinValue<int64_t>(target_manifests, live_files));
	return result;
}

vector<IcebergManifestListEntry> WriteRewrittenManifests(vector<IcebergManifestListEntry> &&selected,
                                                         const RewriteManifestsSpec &spec, idx_t output_count,
                                                         IcebergSnapshotWriter &writer,
                                                         IcebergCommitState &commit_state, int32_t schema_id) {
	auto &table_metadata = commit_state.GetTableMetadata();
	const bool is_v3 = table_metadata.iceberg_version >= 3;

	//! Entries keep their snapshot ids, sequence numbers and (V3) row ids; only their manifest changes.
	vector<IcebergManifestEntry> entries;
	for (auto &manifest : selected) {
		auto loaded = manifest.HasManifestEntries()
		                  ? std::move(manifest)
		                  : IcebergManifestMerge::ScanManifestEntries(manifest, commit_state, schema_id);
		auto prepared = loaded.GetFile().PrepareEntriesForRewrite(std::move(loaded.GetManifestEntries()));
		for (auto &entry : prepared) {
			//! Tombstones only describe the snapshot that removed the file; a rewrite drops them.
			if (entry.status == IcebergManifestEntryStatusType::DELETED) {
				continue;
			}
			entries.push_back(std::move(entry));
		}
	}
	vector<IcebergManifestListEntry> result;
	if (entries.empty()) {
		return result;
	}

	std::stable_sort(entries.begin(), entries.end(), [&](const IcebergManifestEntry &a, const IcebergManifestEntry &b) {
		return PartitionLessThan(a, b, spec.sort_field_ids);
	});

	auto manifest_count = MaxValue<idx_t>(MinValue<idx_t>(output_count, entries.size()), 1);
	auto manifest_metadata =
	    IcebergManifestMetadata(schema_id, spec.spec_id, NumericCast<int32_t>(table_metadata.iceberg_version),
	                            IcebergManifestContentType::DATA);
	for (idx_t i = 0; i < manifest_count; i++) {
		auto begin = i * entries.size() / manifest_count;
		auto end = (i + 1) * entries.size() / manifest_count;
		vector<IcebergManifestEntry> chunk;
		chunk.reserve(end - begin);
		//! Every entry carries an explicit first_row_id, so the manifest's own value only has to stay within the
		//! table's assigned range; the smallest one never advances next-row-id, as in manifest merging.
		optional<int64_t> first_row_id;
		for (auto idx = begin; idx < end; idx++) {
			auto &entry = entries[idx];
			if (is_v3 && entry.data_file.HasFirstRowId()) {
				auto entry_first_row_id = entry.data_file.GetFirstRowId();
				if (!first_row_id || entry_first_row_id < *first_row_id) {
					first_row_id = entry_first_row_id;
				}
			}
			chunk.push_back(std::move(entry));
		}
		result.push_back(writer.WriteReplacementManifest(manifest_metadata, std::move(chunk), first_row_id));
	}
	return result;
}

IcebergRewriteManifestsSnapshot::IcebergRewriteManifestsSnapshot(const IcebergTable &table_info,
                                                                 RewriteManifestsSpec spec_p,
                                                                 RewriteManifestsResult planned_p)
    : IcebergAddSnapshot(table_info, IcebergSnapshotOperationType::REPLACE), spec(std::move(spec_p)),
      planned(planned_p), schema_id(table_info.table_metadata.GetCurrentSchemaId()) {
}

void IcebergRewriteManifestsSnapshot::CreateUpdate(DatabaseInstance &db, ClientContext &context,
                                                   IcebergCommitState &commit_state) const {
	auto writer = commit_state.CreateRewriteManifestsSnapshotWriter(schema_id, spec, planned.added_manifests_count);
	commit_state.AddWrittenSnapshot(IcebergWrittenSnapshot::Create(std::move(writer)));
}

} // namespace duckdb
