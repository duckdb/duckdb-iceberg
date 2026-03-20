#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/function/copy_function.hpp"

#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "rest_catalog/objects/add_snapshot_update.hpp"
#include "catalog/rest/api/iceberg_table_update.hpp"
#include "catalog/rest/api/iceberg_table_requirement.hpp"
#include "catalog/rest/api/iceberg_add_snapshot.hpp"
#include "catalog/rest/api/iceberg_create_table_request.hpp"
#include "catalog/rest/transaction/iceberg_transaction_metadata.hpp"

namespace duckdb {

struct IcebergTableInformation;
struct IcebergCreateTableRequest;

struct IcebergTransactionData {
public:
	IcebergTransactionData(ClientContext &context, const IcebergTableInformation &table_info);

public:
	void AddSnapshot(IcebergSnapshotOperationType operation, vector<IcebergManifestEntry> &&data_files,
	                 case_insensitive_map_t<IcebergManifestDeletes> &&altered_manifests);
	void AddUpdateSnapshot(vector<IcebergManifestEntry> &&delete_files, vector<IcebergManifestEntry> &&data_files,
	                       case_insensitive_map_t<IcebergManifestDeletes> &&altered_manifests);
	// add a schema update for a table
	void TableAddSchema();
	void TableAddAssertCreate();
	void TableAddAssertCurrentSchemaId();
	void TableAddAssertLastAssignedColumnFieldId();
	void TableAddAssertLastAssignedPartitionId();
	void TableAddAssertDefaultSpecId();
	void TableAssignUUID();
	void TableAddUpradeFormatVersion();
	void TableAddSetCurrentSchema();
	void TableAddPartitionSpec();
	void TableAddSortOrder();
	void TableSetDefaultSortOrder();
	void TableSetDefaultSpec();
	void TableSetProperties(const case_insensitive_map_t<string> &properties);
	void TableRemoveProperties(const vector<string> &properties);
	void TableSetLocation();

private:
	void CacheExistingManifestList(lock_guard<mutex> &guard, const IcebergTableMetadata &metadata);

public:
	ClientContext &context;
	const IcebergTableInformation &table_info;
	//! schema updates etc.
	vector<unique_ptr<IcebergTableUpdate>> updates;
	//! has the table been deleted in the current transaction
	bool is_deleted;
	vector<unique_ptr<IcebergTableRequirement>> requirements;
	//! Cached manifest list from the source snapshot
	vector<IcebergManifestListEntry> existing_manifest_list;

	//! Every insert/update/delete creates an alter of the table data
	vector<reference<IcebergAddSnapshot>> alters;
	//! The 'referenced_data_file' -> 'data_file.file_path' of the currently active transaction-local deletes
	case_insensitive_map_t<string> transactional_delete_files;
	//! Track the current row id for this transaction
	int64_t next_row_id = 0;

	mutex lock;
};

} // namespace duckdb
