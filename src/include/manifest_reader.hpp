#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"

#include "iceberg_options.hpp"

#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_manifest_list.hpp"

#include "avro_scan.hpp"

namespace duckdb {

// Manifest Reader

class BaseManifestReader {
public:
	BaseManifestReader(idx_t iceberg_version) : iceberg_version(iceberg_version) {
	}
	virtual ~BaseManifestReader() {
	}

public:
	void Initialize(unique_ptr<AvroScan> scan_p);
	bool Finished() const;

protected:
	idx_t ScanInternal(idx_t remaining);
	const IcebergAvroScanInfo &GetScanInfo() const;

protected:
	DataChunk chunk;
	const idx_t iceberg_version;
	unique_ptr<AvroScan> scan;
	idx_t offset = 0;
	bool finished = true;
};

namespace manifest_list {

//! Produces IcebergManifests read, from the 'manifest_list'
class ManifestListReader : public BaseManifestReader {
public:
	ManifestListReader(idx_t iceberg_version);
	~ManifestListReader() override {
	}

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
	ManifestReader(idx_t iceberg_version, bool skip_deleted = true);
	~ManifestReader() override {
	}

public:
	idx_t Read(idx_t count, vector<IcebergManifestEntry> &result);

public:
private:
	idx_t ReadChunk(idx_t offset, idx_t count, vector<IcebergManifestEntry> &result);

public:
	//! Whether the deleted entries should be skipped outright
	bool skip_deleted = false;
};

} // namespace manifest_file

} // namespace duckdb
