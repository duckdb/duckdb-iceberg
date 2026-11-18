//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/transaction/iceberg_commit_plan.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"

#include "catalog/iceberg_catalog_types.hpp"
#include "catalog/rest/api/iceberg_retry.hpp"

namespace duckdb {

class IcebergTransaction;
struct IcebergTable;

//! One commit attempt of one or more tables, staged against their current metadata
struct IcebergStagedCommit {
	vector<IcebergTableChange> changes;
	unordered_set<string> table_keys;
	//! The manifests and manifest lists this attempt wrote
	vector<string> written_files;
	bool retryable = false;
	IcebergRetryConfig retry_config;
};

//! A table a backend published ahead of the commit, and the generation it published
struct IcebergCreatedTableRecord {
	string table_key;
	IcebergEntryId id;
	shared_ptr<const IcebergLoadedTable> created;
};

//! The one table change a transaction can make besides creating tables: an update of one or more tables, a drop or
//! a rename
enum class IcebergTableChangeKind : uint8_t { NONE, UPDATE, DROP, RENAME };

//! What a transaction publishes at commit, as a backend sees it. The plan reads the transaction; the backend calls
//! the On...() methods after it published a change, so the catalog's in-memory state (schema set, table sets, the
//! backend's caches) follows. Staging writes the manifests and manifest lists of an attempt and returns the spec's
//! requirements and updates; it is independent of how the backend publishes them.
class IcebergCommitPlan {
public:
	explicit IcebergCommitPlan(IcebergTransaction &transaction);

public:
	//===--------------------------------------------------------------------===//
	// Namespaces
	//===--------------------------------------------------------------------===//
	const vector<IcebergNamespace> &CreatedNamespaces() const {
		return created_namespaces;
	}
	//! The namespace at 'index' of CreatedNamespaces was created, or existed already
	void OnNamespaceCreated(idx_t index, bool already_existed);
	vector<IcebergNamespacePropertyUpdate> NamespacePropertyUpdates() const;
	const vector<IcebergNamespace> &DroppedNamespaces() const {
		return dropped_namespaces;
	}
	void OnNamespaceDropped(idx_t index);
	void OnNamespacesDropped();

	//===--------------------------------------------------------------------===//
	// Tables
	//===--------------------------------------------------------------------===//
	IcebergTableChangeKind TableChangeKind() const;
	//! Whether there is a table with changes to commit (UPDATE)
	bool HasTableUpdates() const;
	//! Whether the updated tables can be published by one atomic multi-table commit (UPDATE)
	bool CanCommitTablesAtomically() const;
	//! Stages one attempt of every updated table. Returns false if there is nothing to commit (UPDATE)
	bool StageTables(ClientContext &context, IcebergStagedCommit &staged);
	//! Stages one attempt of the one updated table. Returns false if there is nothing to commit (UPDATE)
	bool StageTable(ClientContext &context, IcebergStagedCommit &staged);
	void OnTablesUpdated();
	//! The dropped table (DROP)
	IcebergEntryId DroppedTable() const;
	void OnTableDropped(ClientContext &context);
	//! The renamed table and its new name (RENAME)
	IcebergEntryId RenamedTable() const;
	string NewTableName() const;
	void OnTableRenamed();
	//! The tables the backend published ahead of the commit, which an abort drops
	const vector<IcebergCreatedTableRecord> &CreatedTables() const;

	//===--------------------------------------------------------------------===//
	// Views
	//===--------------------------------------------------------------------===//
	vector<IcebergEntryId> DroppedViews() const;
	void OnViewsDropped();
	vector<IcebergNewView> CreatedViews(ClientContext &context) const;
	void OnViewsCreated();

	//===--------------------------------------------------------------------===//
	// Retries and the outcome
	//===--------------------------------------------------------------------===//
	//! Removes the files of a definitely failed attempt, if the backend allows deleting written files
	void RemoveWrittenFiles(ClientContext &context, const vector<string> &paths);
	//! Reloads the tables of a conflicting attempt, before they are staged again
	void RefreshForRetry(ClientContext &context, const unordered_set<string> &table_keys);
	//! The changes may have been published: keep the written files and the created tables
	void MarkOutcomeUnknown();

private:
	IcebergTransaction &transaction;
	vector<string> created_namespace_names;
	vector<IcebergNamespace> created_namespaces;
	vector<string> dropped_namespace_names;
	vector<IcebergNamespace> dropped_namespaces;
	//! The one updated table of a single-table commit, resolved on the first attempt
	optional_ptr<IcebergTable> single_table;
	bool single_table_resolved = false;
};

//! Retries a commit whose attempts fail with a CONFLICT, which the spec defines as "nothing was published": up to
//! 'commit.retry.num-retries' times, within 'commit.retry.total-timeout-ms', and only if every staged change can be
//! rebased. Each attempt is staged again, against the reloaded tables. Shared by every backend.
class IcebergCommitRetryLoop {
public:
	//! Stages an attempt; returns false if there is nothing to commit
	using StageFunction = std::function<bool(IcebergStagedCommit &staged)>;
	//! Publishes an attempt; returns the error if it failed
	using PublishFunction = std::function<optional<IcebergCatalogError>(IcebergStagedCommit &staged)>;

	static void Run(ClientContext &context, IcebergCommitPlan &plan, const StageFunction &stage,
	                const PublishFunction &publish);
};

} // namespace duckdb
