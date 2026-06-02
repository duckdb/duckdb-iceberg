#include "planning/metadata_io/manifest/bound_iceberg_manifest_entry.hpp"

namespace duckdb {

BoundIcebergManifestEntry::BoundIcebergManifestEntry(idx_t file_idx, const IcebergManifestEntry &entry)
    : manifest_file_idx(file_idx), entry(entry), has_first_row_id(false) {
}

BoundIcebergManifestEntry::BoundIcebergManifestEntry(idx_t file_idx, const IcebergManifestEntry &entry,
                                                     int64_t first_row_id)
    : manifest_file_idx(file_idx), entry(entry), has_first_row_id(true), first_row_id(first_row_id) {
}

int64_t BoundIcebergManifestEntry::GetFirstRowId() const {
	D_ASSERT(has_first_row_id);
	return first_row_id;
}

bool BoundIcebergManifestEntry::HasFirstRowId() const {
	return has_first_row_id;
}

} // namespace duckdb
