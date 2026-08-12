#pragma once

#include "duckdb/common/optional.hpp"
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

struct IcebergTable;
struct IcebergCreateTableRequest;

struct IcebergTransactionData {
public:
	IcebergTransactionData(ClientContext &context, IcebergTransaction &transaction, const IcebergTable &table_info);

public:
	int64_t GetCommitRetryCount() const;
	bool SupportsAppendRetry() const;
	bool RetryStateMatches(const IcebergTable &table_info) const;
	//! Whether this transaction stages a DELETE snapshot; gates the commit-retry safety check.
	bool ContainsDelete() const;
	bool IsFileInvalidated(const string &file_path) const;

	void AddSnapshot(IcebergSnapshotOperationType operation, vector<IcebergManifestEntry> &&data_files,
	                 IcebergManifestDeletes &&altered_manifests);
	void AddDeleteSnapshot(partitioned_manifest_entry_map_t &&delete_files, IcebergManifestDeletes &&altered_manifests);
	void AddUpdateSnapshot(partitioned_manifest_entry_map_t &&delete_files, vector<IcebergManifestEntry> &&data_files,
	                       IcebergManifestDeletes &&altered_manifests);
	// add a schema update for a table
	void TableAddSchema(int32_t schema_id);
	void TableSetCurrentSchema(int32_t schema_id);
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
	//! Roll main back to an ancestor snapshot (Spark rollback_to_snapshot semantics).
	void TableRollbackToSnapshot(int64_t snapshot_id);

private:
	void CacheExistingManifestList(lock_guard<mutex> &guard, const IcebergTableMetadata &metadata);
	//! Writes one delete manifest per partition spec present in 'delete_files'.
	void AddDeleteManifestFiles(IcebergAddSnapshot &add_snapshot, partitioned_manifest_entry_map_t &&delete_files,
	                            sequence_number_t sequence_number);
	void AddSnapshotUpdate(unique_ptr<IcebergAddSnapshot> add_snapshot, IcebergManifestDeletes &&altered_manifests);

public:
	string initial_table_uuid;
	int32_t initial_schema_id;
	int32_t initial_default_spec_id = 0;
	optional_idx initial_default_sort_order_id;

	ClientContext &context;
	IcebergTransaction &transaction;
	const IcebergTable &table_info;
	//! Transaction-wide invalidated file paths, tagged with the alter that first invalidated them
	IcebergManifestDeletes manifest_deletes;
	//! schema updates etc.
	vector<unique_ptr<IcebergTableUpdate>> updates;
	vector<unique_ptr<IcebergTableRequirement>> requirements;
	//! Cached manifest list from the source snapshot
	vector<IcebergManifestListEntry> existing_manifest_list;

	//! Every insert/update/delete creates an alter of the table data
	vector<reference<IcebergAddSnapshot>> alters;
	//! Snapshot this transaction is based on (the tip when the manifest list was first cached).
	//! Drives the delete commit-retry safety check.
	optional<int64_t> base_snapshot_id;
	//! Track the current row id for this transaction
	int64_t next_row_id = 0;

	//! If we perform an update that relies on the current schema id staying unchanged
	bool assert_schema_id = false;
	//! Whether this transaction explicitly requires the table to be newly created.
	bool has_assert_create = false;
	//! The schema id that should become current when the commit is staged.
	optional<int32_t> pending_current_schema_id;
	mutex lock;
};

} // namespace duckdb
