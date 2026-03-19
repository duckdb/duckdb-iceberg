#pragma once

#include "planning/metadata_io/base_manifest_reader.hpp"

namespace duckdb {

namespace manifest_file {

//! Produces IcebergManifestEntries read, from the 'manifest_file'
class ManifestReader : public BaseManifestReader {
public:
	ManifestReader(const AvroScan &scan, bool skip_deleted);
	~ManifestReader() override;

public:
	idx_t Read(idx_t count, vector<IcebergManifestEntry> &result);

private:
	idx_t ReadChunk(idx_t offset, idx_t count, vector<IcebergManifestEntry> &result);

public:
	//! Whether the deleted entries should be skipped outright
	bool skip_deleted = false;
	//! Map from partition field_id to the partition field name (from the avro schema)
	unordered_map<int32_t, string> partition_field_names;
};

} // namespace manifest_file

} // namespace duckdb
