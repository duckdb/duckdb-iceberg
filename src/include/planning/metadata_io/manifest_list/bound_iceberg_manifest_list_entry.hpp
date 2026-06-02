#pragma once

#include "core/metadata/manifest/iceberg_manifest_list.hpp"

namespace duckdb {

struct BoundIcebergManifestEntry;

struct BoundIcebergManifestListEntry {
public:
	BoundIcebergManifestListEntry(idx_t index, const IcebergManifestListEntry &entry);

public:
	BoundIcebergManifestEntry BindEntry(const IcebergManifestEntry &entry) const;

public:
	const IcebergManifestListEntry &entry;

private:
	const idx_t index;
	bool has_next_row_id = false;
	mutable idx_t next_row_id;
};

} // namespace duckdb
