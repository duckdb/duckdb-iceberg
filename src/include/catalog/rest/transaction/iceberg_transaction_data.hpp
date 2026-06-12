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
#include "catalog/rest/api/table_update.hpp"
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
	                 IcebergManifestDeletes &&altered_manifests);
	void AddUpdateSnapshot(vector<IcebergManifestEntry> &&delete_files, vector<IcebergManifestEntry> &&data_files,
	                       IcebergManifestDeletes &&altered_manifests);
	// add a schema update for a table
	void TableAddSchema(int32_t schema_id);
	void TableSetCurrentSchema();
	void TableAddAssertCreate();
	void TableAddAssertUUID();
	void TableAddAssertCurrentSchemaId();
	void TableAddAssertLastAssignedFieldId();
	void TableAddAssertLastAssignedPartitionId();
	void TableAddAssertDefaultSpecId();
	void TableAssignUUID();
	void TableAddUpradeFormatVersion();
	void TableAddPartitionSpec();
	void TableAddSortOrder();
	void TableSetDefaultSortOrder();
	void TableSetDefaultSpec();
	void TableSetProperties(const case_insensitive_map_t<string> &properties);
	void TableRemoveProperties(const vector<string> &properties);
	void TableSetLocation();

private:
	//! Read the current snapshot's manifest list into existing_manifest_list. `context` must be a
	//! context with an active transaction at call time, because resolving storage secrets (e.g. S3
	//! SIGV4 / credential_chain) requires one. The operator phase passes its live context; the commit
	//! retry passes the commit-time context (the member `context`, captured at construction during the
	//! operator, is no longer transaction-active during commit).
	void CacheExistingManifestList(lock_guard<mutex> &guard, const IcebergTableMetadata &metadata,
	                               ClientContext &context);

public:
	//! Drop the cached parent manifest list so the next apply re-reads it from the (refreshed)
	//! current snapshot. Required for retry: on a commit conflict the parent has moved, and reusing
	//! the stale cache would miss manifests added by the concurrent commit (design doc B16). `context`
	//! must have an active transaction (the commit-time context), used for the manifest re-read.
	void RefreshForRetry(ClientContext &context);

public:
	int32_t initial_schema_id;

	ClientContext &context;
	const IcebergTableInformation &table_info;
	//! schema updates etc.
	vector<unique_ptr<IcebergTableUpdate>> updates;
	vector<unique_ptr<IcebergTableRequirement>> requirements;
	//! Cached manifest list from the source snapshot
	vector<IcebergManifestListEntry> existing_manifest_list;
	//! Whether existing_manifest_list has been read for this transaction (reset on retry).
	bool manifest_list_cached = false;

	//! The snapshot id of the table at the time this transaction first started applying (the parent
	//! of our very first attempt). Captured once and kept stable across retries (NOT reset by
	//! RefreshForRetry, unlike manifest_list_cached). Used by the retry-time conflict validation to
	//! (a) walk the refreshed parent's ancestry back to this id -- detecting a concurrent rollback
	//! that would make a retry validate against unrelated history -- and (b) bound the set of
	//! concurrently-added snapshots to inspect. -1 means "not yet captured".
	int64_t starting_snapshot_id = -1;
	bool starting_snapshot_captured = false;

	//! Every insert/update/delete creates an alter of the table data
	vector<reference<IcebergAddSnapshot>> alters;
	//! The 'referenced_data_file' -> 'data_file.file_path' of the currently active transaction-local deletes
	case_insensitive_map_t<string> transactional_delete_files;
	//! Track the current row id for this transaction
	int64_t next_row_id = 0;
	//! Paths of manifest / manifest-list files written across all commit attempts for this table.
	//! Used by CleanupFiles to delete metadata files left behind by a failed transaction or by
	//! discarded retry attempts. Never used on a successful commit.
	vector<string> written_metadata_paths;

	//! If we perform an update that relies on the current schema id staying unchanged
	bool assert_schema_id = false;
	//! Whether the current schema of the table should be updated
	bool set_schema_id = false;
	mutex lock;
};

} // namespace duckdb
