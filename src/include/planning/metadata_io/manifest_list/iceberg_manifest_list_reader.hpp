#pragma once

#include "planning/metadata_io/base_manifest_reader.hpp"

namespace duckdb {

namespace manifest_list {

//! Produces IcebergManifests read, from the 'manifest_list'
class ManifestListReader : public BaseManifestReader {
public:
	ManifestListReader(const AvroScan &scan);
	~ManifestListReader() override;

public:
	idx_t Read(idx_t count, vector<IcebergManifestListEntry> &result);

private:
	idx_t ReadChunk(idx_t offset, idx_t count, vector<IcebergManifestListEntry> &result);
};

} // namespace manifest_list

} // namespace duckdb
