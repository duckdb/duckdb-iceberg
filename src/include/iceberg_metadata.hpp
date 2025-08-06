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
	IcebergScanInfo(const string &metadata_path, IcebergTableMetadata &metadata, optional_ptr<IcebergSnapshot> snapshot,
	                IcebergTableSchema &schema)
	    : metadata_path(metadata_path), metadata(metadata), snapshot(snapshot), schema(schema) {
	}
	IcebergScanInfo(const string &metadata_path, unique_ptr<IcebergScanTemporaryData> owned_temp_data_p,
	                optional_ptr<IcebergSnapshot> snapshot, IcebergTableSchema &schema)
	    : metadata_path(metadata_path), owned_temp_data(std::move(owned_temp_data_p)),
	      metadata(owned_temp_data->metadata), snapshot(snapshot), schema(schema) {
	}

public:
	string metadata_path;
	unique_ptr<IcebergScanTemporaryData> owned_temp_data;
	IcebergTableMetadata &metadata;
	optional_ptr<IcebergTransactionData> transaction_data;

	optional_ptr<IcebergSnapshot> snapshot;
	IcebergTableSchema &schema;
};

//! ------------- ICEBERG_METADATA TABLE FUNCTION -------------

struct IcebergTableEntry {
public:
	IcebergTableEntry(IcebergManifest &&manifest, IcebergManifestFile &&manifest_file)
	    : manifest(std::move(manifest)), manifest_file(std::move(manifest_file)) {
	}

public:
	IcebergManifest manifest;
	IcebergManifestFile manifest_file;
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
	//! Returns all paths to be scanned for the IcebergManifestContentType
	template <IcebergManifestContentType TYPE>
	vector<string> GetPaths() {
		vector<string> ret;
		for (auto &table_entry : entries) {
			if (table_entry.manifest.content != TYPE) {
				continue;
			}
			for (auto &data_file : table_entry.manifest_file.data_files) {
				if (data_file.status == IcebergManifestEntryStatusType::DELETED) {
					continue;
				}
				ret.push_back(data_file.file_path);
			}
		}
		return ret;
	}
	vector<IcebergManifestEntry> GetAllPaths() {
		vector<IcebergManifestEntry> ret;
		for (auto &table_entry : entries) {
			for (auto &manifest_entry : table_entry.manifest_file.data_files) {
				if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
					continue;
				}
				ret.push_back(manifest_entry);
			}
		}
		return ret;
	}

	//! The snapshot of this table
	const IcebergSnapshot &snapshot;
	//! The entries (manifests) of this table
	vector<IcebergTableEntry> entries;

protected:
	string path;
};

} // namespace duckdb
