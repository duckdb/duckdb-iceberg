#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"

#include "iceberg_options.hpp"

#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "iceberg_avro_multi_file_list.hpp"

namespace duckdb {

class AvroScan;

class BaseManifestReader {
public:
	BaseManifestReader(const AvroScan &scan);
	virtual ~BaseManifestReader();

public:
	bool Finished() const;

protected:
	idx_t ScanInternal(idx_t remaining);
	const IcebergAvroScanInfo &GetScanInfo() const;

private:
	void InitializeInternal();

protected:
	const AvroScan &scan;
	DataChunk chunk;
	unique_ptr<LocalTableFunctionState> local_state;
	const idx_t iceberg_version;
	idx_t offset = 0;
	bool initialized = false;
	bool finished = false;
};

namespace manifest_list {

//! Produces IcebergManifests read, from the 'manifest_list'
class ManifestListReader : public BaseManifestReader {
public:
	ManifestListReader(const AvroScan &scan);
	~ManifestListReader() override;

public:
	idx_t Read(idx_t count, vector<IcebergManifestFile> &result);

private:
	idx_t ReadChunk(idx_t offset, idx_t count, vector<IcebergManifestFile> &result);
};

} // namespace manifest_list

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
};

} // namespace manifest_file

} // namespace duckdb
