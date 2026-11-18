#pragma once

#include "planning/metadata_io/base_manifest_reader.hpp"

#include <functional>

namespace duckdb {

struct IcebergManifestReaderInput {
public:
	IcebergManifestReaderInput(const IcebergManifestMetadata &metadata, const IcebergPartitionSpec &partition_spec,
	                           int32_t table_format_version)
	    : metadata(metadata), partition_spec(partition_spec), table_format_version(table_format_version) {
	}

public:
	const IcebergManifestMetadata &metadata;
	const IcebergPartitionSpec &partition_spec;
	const int32_t table_format_version;
};

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

public:
	static void ReadChunk(DataChunk &chunk, const map<idx_t, LogicalType> &partition_field_id_to_type,
	                      IcebergManifestReaderInput &reader_input, vector<IcebergManifestEntry> &result);

private:
	optional_idx current_batch;
};

} // namespace manifest_file

} // namespace duckdb
