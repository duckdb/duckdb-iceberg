#pragma once

#include "core/metadata/manifest/iceberg_manifest_list.hpp"

namespace duckdb {

//! Content retained by a transaction until a commit attempt assigns a manifest path and identity.
class IcebergPendingManifest {
public:
	IcebergPendingManifest(IcebergManifestMetadata metadata, vector<IcebergManifestEntry> entries)
	    : metadata(std::move(metadata)), entries(std::move(entries)) {
	}

	const IcebergManifestMetadata &GetMetadata() const {
		return metadata;
	}
	const vector<IcebergManifestEntry> &GetEntries() const {
		return entries;
	}

	//! Adapt to the scanner's manifest representation without allocating a file path.
	//! Sequence numbers belong to this scan, not to the pending content. The scan gets no first_row_id.
	IcebergManifestListEntry CreateScanEntry(const IcebergTableMetadata &table_metadata,
	                                         sequence_number_t sequence_number) const;

private:
	IcebergManifestMetadata metadata;
	vector<IcebergManifestEntry> entries;
};

} // namespace duckdb
