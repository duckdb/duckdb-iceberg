//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_metadata.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "yyjson.hpp"

#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "iceberg_options.hpp"
#include "rest_catalog/objects/table_metadata.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "core/metadata/schema/iceberg_field_mapping.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"

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
	vector<IcebergManifestListEntry> entries;

protected:
	string path;
};

} // namespace duckdb
