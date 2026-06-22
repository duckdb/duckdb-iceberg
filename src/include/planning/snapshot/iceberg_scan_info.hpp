#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/optional_ptr.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "planning/snapshot/iceberg_snapshot_scan_info.hpp"

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

struct IcebergTransactionData;

//! Used when we are not scanning from a REST Catalog
struct IcebergScanTemporaryData {
	IcebergTableMetadata metadata;
};

struct IcebergScanInfo : public TableFunctionInfo {
public:
	IcebergScanInfo(const string &metadata_path, const IcebergTableMetadata &metadata,
	                IcebergSnapshotScanInfo snapshot_info, const IcebergTableSchema &schema)
	    : metadata_path(metadata_path), metadata(metadata), snapshot_info(snapshot_info), schema(schema) {
	}
	IcebergScanInfo(const string &metadata_path, unique_ptr<IcebergScanTemporaryData> owned_temp_data_p,
	                IcebergSnapshotScanInfo snapshot_info, const IcebergTableSchema &schema)
	    : metadata_path(metadata_path), owned_temp_data(std::move(owned_temp_data_p)),
	      metadata(owned_temp_data->metadata), snapshot_info(snapshot_info), schema(schema) {
	}

public:
	string metadata_path;
	unique_ptr<IcebergScanTemporaryData> owned_temp_data;
	const IcebergTableMetadata &metadata;
	optional_ptr<IcebergTransactionData> transaction_data;

	IcebergSnapshotScanInfo snapshot_info;
	const IcebergTableSchema &schema;

	unique_ptr<ParsedExpression> mandatory_lf_filter_parsed;
	unique_ptr<Expression> mandatory_lf_filter_bound;
};

} // namespace duckdb
