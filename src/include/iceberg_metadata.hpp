//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_metadata.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "yyjson.hpp"

#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_manifest_list.hpp"

#include "iceberg_options.hpp"

#include "duckdb/common/open_file_info.hpp"
#include "duckdb/function/table_function.hpp"

#include "rest_catalog/objects/table_metadata.hpp"

#include "metadata/iceberg_snapshot.hpp"
#include "metadata/iceberg_table_metadata.hpp"
#include "metadata/iceberg_partition_spec.hpp"
#include "metadata/iceberg_table_schema.hpp"
#include "metadata/iceberg_field_mapping.hpp"
#include "metadata/iceberg_manifest.hpp"

#include "storage/iceberg_transaction_data.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

//! Used when we are not scanning from a REST Catalog
struct IcebergScanTemporaryData {
	IcebergTableMetadata metadata;
};

struct IcebergTransactionData;

struct IcebergScanInfo : public TableFunctionInfo {
public:
	IcebergScanInfo(const string &metadata_path, const IcebergTableMetadata &metadata,
	                optional_ptr<const IcebergSnapshot> snapshot, const IcebergTableSchema &schema)
	    : metadata_path(metadata_path), metadata(metadata), snapshot(snapshot), schema(schema) {
	}
	IcebergScanInfo(const string &metadata_path, unique_ptr<IcebergScanTemporaryData> owned_temp_data_p,
	                optional_ptr<const IcebergSnapshot> snapshot, const IcebergTableSchema &schema)
	    : metadata_path(metadata_path), owned_temp_data(std::move(owned_temp_data_p)),
	      metadata(owned_temp_data->metadata), snapshot(snapshot), schema(schema) {
	}

public:
	string metadata_path;
	unique_ptr<IcebergScanTemporaryData> owned_temp_data;
	const IcebergTableMetadata &metadata;
	optional_ptr<IcebergTransactionData> transaction_data;

	optional_ptr<const IcebergSnapshot> snapshot;
	const IcebergTableSchema &schema;
};

//! ------------- ICEBERG_METADATA TABLE FUNCTION -------------

struct IcebergTableManifestEntry {
public:
	IcebergTableManifestEntry(IcebergManifestFile &&manifest, IcebergManifest &&manifest_file)
	    : manifest(std::move(manifest)), manifest_file(std::move(manifest_file)) {
	}
	IcebergTableManifestEntry(IcebergManifestFile &&manifest_p)
	    : manifest(manifest_p), manifest_file(manifest.manifest_path) {
	}

public:
	IcebergManifestFile manifest;
	IcebergManifest manifest_file;
};

struct IcebergTable {
public:
	IcebergTable(const IcebergSnapshot &snapshot);

public:
	//! Loads all(!) metadata of into IcebergTable object
	static unique_ptr<IcebergTable> Load(const string &iceberg_path, const IcebergTableMetadata &metadata,
	                                     const IcebergSnapshot &snapshot, ClientContext &context,
	                                     const IcebergOptions &options);

public:
	//! The snapshot of this table
	const IcebergSnapshot &snapshot;
	vector<IcebergTableManifestEntry> entries;

protected:
	string path;
};

} // namespace duckdb
