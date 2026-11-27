#pragma once

#include "planning/metadata_io/base_manifest_reader.hpp"

#include <functional>

namespace duckdb {

namespace manifest_file {

class ManifestReader : public BaseManifestReader {
public:
	ManifestReader(const AvroScan &scan);
	~ManifestReader() override;

public:
	void Read();
	static void ReadPaths(vector<IcebergManifestListEntry> &manifest_files, const IcebergTableMetadata &metadata,
	                      ClientContext &context, const IcebergFileSecret &file_secret,
	                      const std::function<void(const string &)> &visit_path);

private:
	void ReadChunk(const IcebergManifestFileScanInfo &scan_info, const IcebergManifestMetadata &manifest,
	               vector<IcebergManifestEntry> &result);
	optional_idx current_batch;
};

} // namespace manifest_file

} // namespace duckdb
