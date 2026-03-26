#include "planning/metadata_io/manifest_list/bound_iceberg_manifest_list_entry.hpp"
#include "planning/metadata_io/manifest/bound_iceberg_manifest_entry.hpp"

namespace duckdb {

BoundIcebergManifestListEntry::BoundIcebergManifestListEntry(idx_t index, const IcebergManifestListEntry &entry)
    : entry(entry), index(index) {
	has_next_row_id = entry.file.has_first_row_id;
	if (has_next_row_id) {
		//! 'first_row_id' is NULL for pre-V3 manifests
		next_row_id = entry.file.first_row_id;
	}
}

BoundIcebergManifestEntry BoundIcebergManifestListEntry::BindEntry(const IcebergManifestEntry &entry) const {
	auto &data_file = entry.data_file;
	if (data_file.HasFirstRowId()) {
		return BoundIcebergManifestEntry(index, entry, data_file.GetFirstRowId());
	}
	if (has_next_row_id) {
		auto res = BoundIcebergManifestEntry(index, entry, next_row_id);
		next_row_id += data_file.record_count;
		return res;
	}
	return BoundIcebergManifestEntry(index, entry);
}

} // namespace duckdb
