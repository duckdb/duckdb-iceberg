#pragma once

#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_snapshot.hpp"
#include "rest_catalog/objects/add_snapshot_update.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/function/copy_function.hpp"
#include "storage/iceberg_table_update.hpp"
#include "storage/iceberg_table_requirement.hpp"
#include "storage/table_update/iceberg_add_snapshot.hpp"
#include "storage/table_create/iceberg_create_table_request.hpp"

namespace duckdb {

struct IcebergTableInformation;
struct IcebergCreateTableRequest;

struct IcebergTransactionData {
public:
	IcebergTransactionData(ClientContext &context, IcebergTableInformation &table_info)
	    : context(context), table_info(table_info), is_deleted(false) {
	}

public:
	void AddSnapshot(IcebergSnapshotOperationType operation, vector<IcebergManifestEntry> &&data_files);
	// add a schema update for a table
	void TableAddSchema();
	void TableAddAssertCreate();
	void TableAssignUUID();
	void TableAddUpradeFormatVersion();
	void TableAddSetCurrentSchema();
	void TableAddPartitionSpec();
	void TableAddSortOrder();
	void TableSetDefaultSortOrder();
	void TableSetDefaultSpec();
	void TableSetProperties(case_insensitive_map_t<string> properties);
	void TableSetLocation();

public:
	ClientContext &context;
	IcebergTableInformation &table_info;
	vector<unique_ptr<IcebergTableUpdate>> updates;
	//! has the table been deleted in the current transaction
	bool is_deleted;
	vector<unique_ptr<IcebergTableRequirement>> requirements;

	//! Every insert/update/delete creates an alter of the table data
	vector<reference<IcebergAddSnapshot>> alters;
};

} // namespace duckdb
