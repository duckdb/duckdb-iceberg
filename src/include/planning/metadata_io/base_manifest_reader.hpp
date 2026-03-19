#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"

#include "iceberg_options.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "planning/metadata_io/avro/iceberg_avro_multi_file_list.hpp"

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

} // namespace duckdb
