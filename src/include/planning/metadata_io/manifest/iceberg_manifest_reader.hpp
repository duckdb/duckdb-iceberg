#pragma once

#include "planning/metadata_io/base_manifest_reader.hpp"

namespace duckdb {

namespace manifest_file {

class ManifestReader : public BaseManifestReader {
public:
	ManifestReader(const AvroScan &scan);
	~ManifestReader() override;

public:
	void Read();

private:
	void ReadChunk(const IcebergManifestFileScanInfo &scan_info, const IcebergManifestMetadata &manifest,
	               vector<IcebergManifestEntry> &result);
	optional_idx current_batch;
};

} // namespace manifest_file

} // namespace duckdb
