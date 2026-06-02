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
	void Read();

public:
	static void ReadChunk(DataChunk &chunk, idx_t iceberg_version, vector<IcebergManifestListEntry> &result);
};

} // namespace manifest_list

} // namespace duckdb
