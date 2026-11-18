#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/common/http_util.hpp"

#include "catalog/iceberg_catalog_backend.hpp"
#include "catalog/rest/iceberg_schema_set.hpp"
#include "common/iceberg_utils.hpp"
#include "iceberg_attach.hpp"

namespace duckdb {

class IcebergSchemaEntry;
struct IcebergTable;

class IcebergCatalog : public Catalog {
public:
	explicit IcebergCatalog(AttachedDatabase &db_p, AccessMode access_mode, IcebergAttachOptions &attach_options,
	                        string attach_path, unique_ptr<IcebergCatalogBackend> backend);
	~IcebergCatalog() override;

public:
	static unique_ptr<SecretEntry> GetStorageSecret(ClientContext &context, const string &secret_name);
	static unique_ptr<SecretEntry> GetIcebergSecret(ClientContext &context, const string &secret_name);
	static unique_ptr<SecretEntry> GetHTTPSecret(ClientContext &context, const string &secret_name);
	//! The backend that talks to the catalog service
	IcebergCatalogBackend &GetBackend() const {
		return *backend;
	}
	//! The key that identifies a table or view of this catalog in caches and transactions
	string GetEntryKey(const vector<string> &namespace_items, const string &name) const;
	//! Whether or not this catalog should search a specific type with the standard priority
	CatalogLookupBehavior CatalogTypeLookupRule(CatalogType type) const override {
		switch (type) {
		case CatalogType::TABLE_FUNCTION_ENTRY:
		case CatalogType::SCALAR_FUNCTION_ENTRY:
		case CatalogType::AGGREGATE_FUNCTION_ENTRY:
			return CatalogLookupBehavior::NEVER_LOOKUP;
		default:
			return CatalogLookupBehavior::STANDARD;
		}
	}
	bool CheckAmbiguousCatalogOrSchema(ClientContext &context, const Identifier &schema) override {
		return false;
	}
	optional<Identifier> GetDefaultSchema() const override;
	ErrorData SupportsCreateTable(BoundCreateTableInfo &info) override;
	//! Applies IDENTIFIER_CASE to a namespace or table name
	string FoldIdentifier(const string &name) const;
	//! Applies IDENTIFIER_CASE to the schema and name parts, keeping the catalog alias as written
	QualifiedName FoldName(const QualifiedName &name) const;

public:
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "iceberg";
	}
	bool SupportsTimeTravel() const override {
		return true;
	}
	void DropSchema(ClientContext &context, DropInfo &info) override;
	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	IcebergSchemaSet &GetSchemas();
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	//! Plans a copy-on-write DELETE itself; any other DELETE plans its child and continues below
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	//! Shared delete-planning body both PlanDelete and MERGE build on; only PlanDelete additionally opts the
	//! standalone DELETE into metadata-only deletes, so MERGE must plan its delete action through here directly.
	PhysicalOperator &PlanDeleteOperation(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                                      PhysicalOperator &plan);
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner, LogicalMergeInto &op,
	                                PhysicalOperator &plan) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;
	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	//! Whether or not this is an in-memory Iceberg database
	bool InMemory() override;
	string GetDBPath() override;
	//! Allow ATTACH OR REPLACE to actually re-attach when iceberg-specific options change
	bool HasConflictingAttachOptions(const string &path, const AttachOptions &options) override;
	void SetAttachOptions(const unordered_map<string, Value> &options);
	//! Throws if the write mode property doesn't allow merge-on-read delete files, which is all DuckDB-Iceberg writes
	static void VerifyMergeOnRead(const IcebergTableMetadata &metadata, const string &table_name,
	                              const string &write_mode_property);

public:
	AccessMode access_mode;
	//! attach options
	IcebergAttachOptions attach_options;
	//! Optionally (user-)provided default schema to use
	optional<Identifier> default_schema;

private:
	//! Original client selector, used for ATTACH OR REPLACE comparisons.
	const string attach_path;
	//! Normalized attach options (after core stripping) used to detect a conflicting ATTACH OR REPLACE
	unordered_map<string, Value> normalized_attach_options;
	unique_ptr<IcebergCatalogBackend> backend;

public:
	IcebergSchemaSet schemas;
};

} // namespace duckdb
