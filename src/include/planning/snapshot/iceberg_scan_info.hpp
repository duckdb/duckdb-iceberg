#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/optional_ptr.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"
#include "planning/iceberg_file_secret.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "planning/snapshot/iceberg_snapshot_scan_info.hpp"

namespace duckdb {

struct IcebergTransactionData;

struct IcebergScanInfo : public TableFunctionInfo {
public:
	IcebergScanInfo(const string &metadata_path, shared_ptr<const IcebergTableMetadata> metadata_owner,
	                IcebergSnapshotScanInfo snapshot_info, const IcebergTableSchema &schema,
	                IcebergFileSecret file_secret = {})
	    : metadata_path(metadata_path), metadata_owner(std::move(metadata_owner)), metadata(*this->metadata_owner),
	      file_secret(std::move(file_secret)), snapshot_info(snapshot_info), schema(schema) {
	}

public:
	string metadata_path;
	shared_ptr<const IcebergTableMetadata> metadata_owner;
	const IcebergTableMetadata &metadata;
	IcebergFileSecret file_secret;
	optional_ptr<IcebergTransactionData> transaction_data;

	//! Catalog scan serialization is an immediate plan copy, scoped to the binding statement.
	hugeint_t serialization_transaction_id;
	optional_idx serialization_query_id;

	IcebergSnapshotScanInfo snapshot_info;
	const IcebergTableSchema &schema;
};

} // namespace duckdb
