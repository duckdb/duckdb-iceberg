#pragma once

#include "planning/metadata_io/base_manifest_reader.hpp"

namespace duckdb {

namespace manifest_file {

//! Produces IcebergManifestEntries read, from the 'manifest_file'
class ManifestReader : public BaseManifestReader {
public:
	ManifestReader(const AvroScan &scan);
	~ManifestReader() override;

public:
	void Read();

public:
	static void ReadChunk(DataChunk &chunk, const map<idx_t, LogicalType> &partition_field_id_to_type,
	                      idx_t iceberg_version, vector<IcebergManifestEntry> &result);
};

} // namespace manifest_file

} // namespace duckdb
