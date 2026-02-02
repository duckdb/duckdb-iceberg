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
	virtual void CreateVectorMapping(idx_t i, MultiFileColumnDefinition &column) = 0;
	virtual bool ValidateVectorMapping() = 0;

protected:
	idx_t ScanInternal(idx_t remaining);

protected:
	DataChunk chunk;
	unordered_map<int32_t, ColumnIndex> vector_mapping;
	unordered_map<int32_t, ColumnIndex> partition_fields;
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
	void CreateVectorMapping(idx_t i, MultiFileColumnDefinition &column) override;
	bool ValidateVectorMapping() override;

private:
	idx_t ReadChunk(idx_t offset, idx_t count, vector<IcebergManifestFile> &result);
};

} // namespace manifest_list

namespace manifest_file {

//! Produces IcebergManifestEntries read, from the 'manifest_file'
class ManifestFileReader : public BaseManifestReader {
public:
	ManifestFileReader(idx_t iceberg_version, bool skip_deleted = true);
	~ManifestFileReader() override {
	}

public:
	idx_t Read(idx_t count, vector<IcebergManifestEntry> &result);
	void CreateVectorMapping(idx_t i, MultiFileColumnDefinition &column) override;
	bool ValidateVectorMapping() override;

public:
	void SetSequenceNumber(sequence_number_t sequence_number);
	void SetPartitionSpecID(int32_t partition_spec_id);

private:
	idx_t ReadChunk(idx_t offset, idx_t count, vector<IcebergManifestEntry> &result);

public:
	//! The sequence number to inherit when the condition to do so is met
	sequence_number_t sequence_number;
	//! The inherited partition spec id (from the 'manifest_file')
	int32_t partition_spec_id;
	//! Whether the deleted entries should be skipped outright
	bool skip_deleted = false;
};

} // namespace manifest_file

} // namespace duckdb
