#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/optional_ptr.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"

namespace duckdb {

struct IcebergTransactionData;

//! Used when we are not scanning from a REST Catalog
struct IcebergScanTemporaryData {
	IcebergTableMetadata metadata;
};

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

} // namespace duckdb
