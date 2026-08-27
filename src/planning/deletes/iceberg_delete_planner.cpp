#include "planning/deletes/iceberg_delete_planner.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"
#include "planning/pruning/iceberg_file_pruner.hpp"
#include "planning/scan_plan/iceberg_scan_plan_provider.hpp"

namespace duckdb {

vector<idx_t>
IcebergDeletePlanner::GetDeleteManifestsForDataFile(const IcebergDeletePlanningContext &context,
                                                    const BoundIcebergManifestEntry &data_manifest_entry) {
	vector<idx_t> result;
	auto &data_manifest = context.data_manifests[data_manifest_entry.manifest_file_idx].entry.file;
	auto file_pruner = IcebergFilePruner(context.context, context.metadata, context.schema, context.table_filters);
	for (idx_t manifest_idx = 0; manifest_idx < context.delete_manifests.size(); manifest_idx++) {
		if (!context.delete_manifest_matches[manifest_idx]) {
			continue;
		}
		auto &delete_manifest = context.delete_manifests[manifest_idx].entry.file;
		if (!file_pruner.DeleteManifestMatchesDataFile(delete_manifest, data_manifest, data_manifest_entry.entry)) {
			continue;
		}
		result.push_back(manifest_idx);
	}
	return result;
}

bool IcebergDeletePlanner::DeleteEntryMatchesFilters(const IcebergDeletePlanningContext &context,
                                                     idx_t delete_manifest_idx,
                                                     const IcebergManifestEntry &delete_manifest_entry) {
	if (!context.delete_manifest_matches[delete_manifest_idx]) {
		return false;
	}
	if (!context.table_filters.HasFilters()) {
		return true;
	}
	auto pushdown =
	    IcebergFilePruner(context.context, context.metadata, context.schema, context.table_filters)
	        .FileMatchesFilter(context.delete_manifests[delete_manifest_idx].entry.file, delete_manifest_entry);
	return pushdown != METADATA_STATS_PUSHDOWN::NO_ROWS_MATCH;
}

bool IcebergDeletePlanner::DeleteEntryAppliesToDataFile(const IcebergDeletePlanningContext &context,
                                                        idx_t delete_manifest_idx,
                                                        const IcebergManifestEntry &delete_manifest_entry,
                                                        const BoundIcebergManifestEntry &data_manifest_entry,
                                                        const partition_value_map_t &data_partition_values) {
	auto &delete_file = delete_manifest_entry.data_file;
	auto &data_file = data_manifest_entry.entry.data_file;
	if (!context.provider.DeleteFileAppliesToDataFile(data_file.file_path, delete_file.file_path)) {
		return false;
	}

	auto &delete_manifest = context.delete_manifests[delete_manifest_idx].entry.file;
	auto &data_manifest = context.data_manifests[data_manifest_entry.manifest_file_idx].entry.file;
	return IcebergFilePruner(context.context, context.metadata, context.schema, context.table_filters)
	    .DeleteFileMatchesDataFile(delete_manifest, delete_manifest_entry, data_manifest, data_manifest_entry.entry,
	                               data_partition_values);
}

shared_ptr<IcebergDeleteData>
IcebergDeletePlanner::GetExistingPositionalDeleteData(const IcebergDeletePlanningContext &context,
                                                      const string &file_path) {
	auto &positional_delete_data = context.provider.PositionalDeleteData();
	auto it = positional_delete_data.find(file_path);
	return it == positional_delete_data.end() ? nullptr : it->second;
}

} // namespace duckdb
