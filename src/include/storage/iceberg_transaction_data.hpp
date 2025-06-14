#pragma once

#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_snapshot.hpp"
#include "rest_catalog/objects/add_snapshot_update.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/function/copy_function.hpp"
#include "storage/iceberg_table_update.hpp"
#include "storage/table_update/iceberg_add_snapshot.hpp"

namespace duckdb {

struct IcebergTableInformation;

struct IcebergTransactionData {
public:
	IcebergTransactionData(ClientContext &context, IcebergTableInformation &table_info)
	    : context(context), table_info(table_info) {
	}

public:
	void AddSnapshot(IcebergSnapshotOperationType operation, vector<IcebergManifestEntry> &&data_files);

public:
	ClientContext &context;
	IcebergTableInformation &table_info;
	vector<unique_ptr<IcebergTableUpdate>> updates;

	//! Every insert/update/delete creates an alter of the table data
	vector<reference<IcebergAddSnapshot>> alters;
};

} // namespace duckdb
