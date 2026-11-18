//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/rest/iceberg_rest_commit.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/transaction/iceberg_commit_plan.hpp"

namespace duckdb {

class IcebergRestBackend;

//! The commit sequence of the REST backend: one request per change, in the order the spec's endpoints allow.
//! 1. create namespaces, then update their properties;
//! 2. tables: one atomic POST transactions/commit when the server supports it and no table is stage-created, a
//!    POST .../tables/{table} otherwise; a DELETE .../tables/{table} for a drop, a POST tables/rename for a rename;
//! 3. view drops, then view creates;
//! 4. namespace drops.
class IcebergRestCommit {
public:
	IcebergRestCommit(IcebergRestBackend &backend, ClientContext &context, IcebergCommitPlan &plan);

public:
	void Run();
	//! Drops the tables a non-staged CREATE TABLE published, while the catalog still holds the version the create
	//! returned. Never throws
	static void DropCreatedTables(IcebergRestBackend &backend, ClientContext &context, IcebergCommitPlan &plan);
	//! Whether a failed commit may have been published: anything but a definite rejection (4xx)
	static bool CommitOutcomeUnknown(const ErrorData &error);

private:
	void CreateNamespaces();
	void UpdateNamespaceProperties();
	void CommitTables();
	void CommitTableUpdates();
	void DropViews();
	void CreateViews();
	void DropNamespaces();
	optional<IcebergCatalogError> PublishTransaction(IcebergStagedCommit &staged);
	optional<IcebergCatalogError> PublishTable(IcebergStagedCommit &staged);

private:
	IcebergRestBackend &backend;
	ClientContext &context;
	IcebergCommitPlan &plan;
};

} // namespace duckdb
