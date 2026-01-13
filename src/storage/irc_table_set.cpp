#include "catalog_api.hpp"
#include "catalog_utils.hpp"
#include "iceberg_logging.hpp"

#include "storage/irc_catalog.hpp"
#include "storage/irc_table_set.hpp"
#include "storage/irc_transaction.hpp"
#include "metadata/iceberg_partition_spec.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/common/enums/http_status_code.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "storage/irc_schema_entry.hpp"
#include "duckdb/parser/parser.hpp"

#include "storage/authorization/sigv4.hpp"
#include "storage/iceberg_table_information.hpp"
#include "storage/authorization/oauth2.hpp"

namespace duckdb {

ICTableSet::ICTableSet(IRCSchemaEntry &schema) : schema(schema), catalog(schema.ParentCatalog()) {
}

bool ICTableSet::FillEntry(ClientContext &context, IcebergTableInformation &table) {
	if (!table.schema_versions.empty()) {
		return true;
	}

	auto &ic_catalog = catalog.Cast<IRCatalog>();

	auto get_table_result = IRCAPI::GetTable(context, ic_catalog, schema, table.name);
	if (get_table_result.has_error) {
		if (get_table_result.error_._error.type == "NoSuchIcebergTableException") {
			return false;
		}
		if (get_table_result.status_ == HTTPStatusCode::Forbidden_403 ||
		    get_table_result.status_ == HTTPStatusCode::Unauthorized_401) {
			return false;
		}
		throw HTTPException(get_table_result.error_._error.message);
	}
	table.load_table_result = std::move(get_table_result.result_);
	table.table_metadata = IcebergTableMetadata::FromTableMetadata(table.load_table_result.metadata);
	auto &schemas = table.table_metadata.schemas;

	//! It should be impossible to have a metadata file without any schema
	D_ASSERT(!schemas.empty());
	for (auto &table_schema : schemas) {
		table.CreateSchemaVersion(*table_schema.second);
	}
	return true;
}

void ICTableSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	lock_guard<mutex> l(entry_lock);
	LoadEntries(context);
	case_insensitive_set_t non_iceberg_tables;
	auto table_namespace = IRCAPI::GetEncodedSchemaName(schema.namespace_items);
	for (auto &entry : entries) {
		auto &table_info = entry.second;
		if (table_info.dummy_entry) {
			// FIXME: why do we need to return the same entry again?
			auto &optional = table_info.dummy_entry.get()->Cast<CatalogEntry>();
			callback(optional);
			continue;
		}

		// create a table entry with fake schema data to avoid calling the LoadTableInformation endpoint for every
		// table while listing schemas
		CreateTableInfo info(schema, table_info.name);
		vector<ColumnDefinition> columns;
		auto col = ColumnDefinition(string("__"), LogicalType::UNKNOWN);
		columns.push_back(std::move(col));
		info.columns = ColumnList(std::move(columns));
		auto table_entry = make_uniq<ICTableEntry>(table_info, catalog, schema, info);
		if (!table_entry->internal) {
			table_entry->internal = schema.internal;
		}
		auto result = table_entry.get();
		if (result->name.empty()) {
			throw InternalException("ICTableSet::CreateEntry called with empty name");
		}
		table_info.dummy_entry = std::move(table_entry);
		auto &optional = table_info.dummy_entry.get()->Cast<CatalogEntry>();
		callback(optional);
	}
	// erase not iceberg tables
	for (auto &entry : non_iceberg_tables) {
		entries.erase(entry);
	}
}

void ICTableSet::LoadEntries(ClientContext &context) {
	if (listed) {
		return;
	}
	auto &ic_catalog = catalog.Cast<IRCatalog>();
	auto tables = IRCAPI::GetTables(context, ic_catalog, schema);

	for (auto &table : tables) {
		auto entry_it = entries.find(table.name);
		if (entry_it == entries.end()) {
			entries.emplace(table.name, IcebergTableInformation(ic_catalog, schema, table.name));
		}
	}
	listed = true;
}

bool ICTableSet::CreateNewEntry(ClientContext &context, IRCatalog &catalog, IRCSchemaEntry &schema,
                                CreateTableInfo &info) {
	auto table_name = info.table;
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		throw InvalidInputException("CREATE OR REPLACE not supported in DuckDB-Iceberg");
	}
	auto &irc_transaction = IRCTransaction::Get(context, catalog);
	bool table_deleted_in_transaction = false;
	for (auto &deleted_entry : irc_transaction.deleted_tables) {
		if (deleted_entry->table_info.name == table_name) {
			table_deleted_in_transaction = true;
			break;
		}
	}
	if (entries.find(table_name) != entries.end() || table_deleted_in_transaction) {
		// table still exists in the catalog.
		// check if table has been deleted in the current transaction
		switch (info.on_conflict) {
		case OnCreateConflict::IGNORE_ON_CONFLICT: {
			if (!table_deleted_in_transaction) {
				return false;
			}
			throw NotImplementedException("Cannot create table previously deleted in the same transaction");
		}
		case OnCreateConflict::ERROR_ON_CONFLICT:
			throw InvalidConfigurationException("Table %s already exists", table_name);
		case OnCreateConflict::ALTER_ON_CONFLICT:
			throw NotImplementedException("Alter on conflict");
		default:
			throw InternalException("Unknown conflict state when creating a table");
		}
	}

	entries.emplace(table_name, IcebergTableInformation(catalog, schema, info.table));
	auto &table_info = entries.find(table_name)->second;

	auto table_entry = make_uniq<ICTableEntry>(table_info, catalog, schema, info);
	auto optional_entry = table_entry.get();

	optional_entry->table_info.schema_versions[0] = std::move(table_entry);
	optional_entry->table_info.table_metadata.schemas[0] =
	    IcebergCreateTableRequest::CreateIcebergSchema(optional_entry);
	optional_entry->table_info.table_metadata.current_schema_id = 0;
	optional_entry->table_info.table_metadata.schemas[0]->schema_id = 0;
	// Immediately create the table with stage_create = true to get metadata & data location(s)
	// transaction commit will either commit with data (OR) create the table with stage_create = false
	auto load_table_result = IRCAPI::CommitNewTable(context, catalog, optional_entry);
	optional_entry->table_info.load_table_result = std::move(load_table_result);
	optional_entry->table_info.table_metadata =
	    IcebergTableMetadata::FromTableMetadata(optional_entry->table_info.load_table_result.metadata);

	if (catalog.attach_options.supports_stage_create) {
		// We have a response from the server for a stage create, we need to also send a number of table
		// updates to finalize creation of the table.
		table_info.AddAssertCreate(irc_transaction);
		table_info.AddAssignUUID(irc_transaction);
		table_info.AddUpradeFormatVersion(irc_transaction);
		table_info.AddSchema(irc_transaction);
		table_info.AddSetCurrentSchema(irc_transaction);
		table_info.AddPartitionSpec(irc_transaction);
		table_info.SetDefaultSpec(irc_transaction);
		table_info.AddSortOrder(irc_transaction);
		table_info.SetDefaultSortOrder(irc_transaction);
		table_info.SetLocation(irc_transaction);
	}
	return true;
}

unique_ptr<ICTableInfo> ICTableSet::GetTableInfo(ClientContext &context, IRCSchemaEntry &schema,
                                                 const string &table_name) {
	throw NotImplementedException("ICTableSet::GetTableInfo");
}

optional_ptr<CatalogEntry> ICTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	lock_guard<mutex> l(entry_lock);
	auto &ic_catalog = catalog.Cast<IRCatalog>();

	auto table_name = lookup.GetEntryName();
	auto entry = entries.find(table_name);
	if (entry == entries.end()) {
		if (!IRCAPI::VerifyTableExistence(context, ic_catalog, schema, table_name)) {
			return nullptr;
		}
		auto it = entries.emplace(table_name, IcebergTableInformation(ic_catalog, schema, table_name));
		entry = it.first;
	}
	if (entry->second.transaction_data && entry->second.transaction_data->is_deleted) {
		return nullptr;
	}
	FillEntry(context, entry->second);
	return entry->second.GetSchemaVersion(lookup.GetAtClause());
}

} // namespace duckdb
