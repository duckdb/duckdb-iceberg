#pragma once

#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "planning/iceberg_row_lineage.hpp"

namespace duckdb {

struct BoundIcebergManifestEntry;

struct BoundIcebergManifestListEntry {
public:
	BoundIcebergManifestListEntry(idx_t index, const IcebergManifestListEntry &entry,
	                              IcebergRowLineageMode row_lineage_mode = IcebergRowLineageMode::COMMITTED);

public:
	BoundIcebergManifestEntry BindEntry(const IcebergManifestEntry &entry) const;

public:
	const IcebergManifestListEntry &entry;
	const IcebergRowLineageMode row_lineage_mode;

private:
	const idx_t index;
	mutable optional<idx_t> next_row_id;
};

} // namespace duckdb
