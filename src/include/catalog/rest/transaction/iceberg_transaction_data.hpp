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
class IcebergMergeDeleteCollector;

struct IcebergTransactionData {
public:
	IcebergTransactionData(ClientContext &context, IcebergTransaction &transaction, const IcebergTable &table_info);

public:
	int64_t GetCommitRetryCount() const;
	bool SupportsAppendRetry() const;
	bool RetryStateMatches(const IcebergTable &table_info) const;
	//! Whether this transaction stages a DELETE snapshot; gates the commit-retry safety check.
	bool ContainsDelete() const;
	bool IsFileInvalidated(const IcebergFileIdentity &file) const;
	shared_ptr<IcebergMergeDeleteCollector> GetMergeDeleteCollector(idx_t action_count);
	void VerifyMergeDeletesComplete() const;

	void AddSnapshot(IcebergSnapshotOperationType operation, vector<IcebergManifestEntry> &&data_files,
	                 IcebergManifestDeletes &&altered_manifests, bool merge_action = false);
	void AddDeleteSnapshot(partitioned_manifest_entry_map_t &&delete_files, IcebergManifestDeletes &&altered_manifests,
	                       bool merge_action = false);
	void AddUpdateSnapshot(partitioned_manifest_entry_map_t &&delete_files, vector<IcebergManifestEntry> &&data_files,
	                       IcebergManifestDeletes &&altered_manifests, bool merge_action = false);
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
	//! Retains one pending delete manifest per partition spec present in 'delete_files'.
	void AddPendingDeleteManifests(IcebergAddSnapshot &add_snapshot, partitioned_manifest_entry_map_t &&delete_files);
	//! Called with lock held, since MERGE action pipelines may finalize concurrently.
	void AddSnapshotUpdate(unique_ptr<IcebergAddSnapshot> add_snapshot, IcebergManifestDeletes &&altered_manifests,
	                       bool merge_action);

	//! Query IDs distinguish MERGE executions within the same transaction.
	optional<transaction_t> merge_query_id;
	//! Index in alters shared by the current MERGE's manifests and file invalidations.
	idx_t merge_alter_index = 0;
	//! The collector is per table and execution, independently of whether the MERGE writes a snapshot.
	optional<transaction_t> merge_delete_query_id;
	shared_ptr<IcebergMergeDeleteCollector> merge_delete_collector;

public:
	string initial_table_uuid;
	int32_t initial_format_version;
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

	//! Each data-changing statement creates an alter; the actions of one MERGE share an alter.
	vector<reference<IcebergAddSnapshot>> alters;
	//! Snapshot this transaction is based on (the tip when the manifest list was first cached).
	//! Drives the delete commit-retry safety check.
	optional<int64_t> base_snapshot_id;
	//! Starting sequence for transaction-local delete applicability, captured with the base manifest list.
	//! This is not an assigned row lineage value.
	sequence_number_t scan_sequence_number = 0;

	//! If we perform an update that relies on the current schema id staying unchanged
	bool assert_schema_id = false;
	//! Whether this transaction explicitly requires the table to be newly created.
	bool has_assert_create = false;
	mutex lock;
};

} // namespace duckdb
