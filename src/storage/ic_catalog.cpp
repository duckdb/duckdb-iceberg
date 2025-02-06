#include "storage/ic_catalog.hpp"
#include "storage/ic_schema_entry.hpp"
#include "storage/ic_transaction.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/execution/operator/schema/physical_create_table.hpp"
#include "ic_create_table_as_op.hpp"

namespace duckdb {

ICCatalog::ICCatalog(AttachedDatabase &db_p, const string &internal_name, AccessMode access_mode,
                     ICCredentials credentials)
    : Catalog(db_p), internal_name(internal_name), access_mode(access_mode), credentials(std::move(credentials)),
      schemas(*this) {
}

ICCatalog::~ICCatalog() = default;

void ICCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> ICCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		DropInfo try_drop;
		try_drop.type = CatalogType::SCHEMA_ENTRY;
		try_drop.name = info.schema;
		try_drop.if_not_found = OnEntryNotFound::RETURN_NULL;
		try_drop.cascade = false;
		schemas.DropSchema(transaction.GetContext(), try_drop);
	}
	return schemas.CreateSchema(transaction.GetContext(), info);
}

void ICCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	return schemas.DropSchema(context, info);
}

void ICCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<ICSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> ICCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name,
                                                      OnEntryNotFound if_not_found, QueryErrorContext error_context) {
	if (schema_name == DEFAULT_SCHEMA) {
		if (default_schema.empty()) {
			throw InvalidInputException("Attempting to fetch the default schema - but no database was "
			                            "provided in the connection string");
		}
		return GetSchema(transaction, default_schema, if_not_found, error_context);
	}
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_name);
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	}
	return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
}

bool ICCatalog::InMemory() {
	return false;
}

string ICCatalog::GetDBPath() {
	return internal_name;
}

DatabaseSize ICCatalog::GetDatabaseSize(ClientContext &context) {
	if (default_schema.empty()) {
		throw InvalidInputException("Attempting to fetch the database size - but no database was provided "
		                            "in the connection string");
	}
	DatabaseSize size;
	return size;
}

void ICCatalog::ClearCache() {
	schemas.ClearEntries();
}

unique_ptr<PhysicalOperator> ICCatalog::PlanInsert(ClientContext &context, LogicalInsert &op,
                                                   unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("ICCatalog PlanInsert");
}

unique_ptr<PhysicalOperator> ICCatalog::PlanCreateTableAs(
	ClientContext &context, LogicalCreateTable &op, unique_ptr<PhysicalOperator> plan) {

	CatalogEntry *catalog_entry = schemas.GetEntry(context, op.info->base->schema).get();
	if (!catalog_entry) {
		throw BinderException("Schema not found");
	}

	auto schemaEntry = static_cast<ICSchemaEntry*>(catalog_entry);
	auto create_table_as = make_uniq<ICCreateTableAsOp>(
        op.types, 
		std::move(op.info),
		schemaEntry,
        op.estimated_cardinality,
		internal_name,
		credentials);

    // If your operator has children (input plan), attach them
    if (plan) {
        create_table_as->children.push_back(std::move(plan));
    }

    return create_table_as;
}

unique_ptr<PhysicalOperator> ICCatalog::PlanDelete(ClientContext &context, LogicalDelete &op,
                                                   unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("ICCatalog PlanDelete");
}
unique_ptr<PhysicalOperator> ICCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                                   unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("ICCatalog PlanUpdate");
}
unique_ptr<LogicalOperator> ICCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
                                                       unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("ICCatalog BindCreateIndex");
}

} // namespace duckdb
