#include "catalog_api.hpp"
#include "catalog_utils.hpp"
#include "iceberg_logging.hpp"

#include "storage/irc_catalog.hpp"
#include "storage/irc_table_set.hpp"

#include "storage/irc_table_entry.hpp"
#include "storage/irc_transaction.hpp"
#include "storage/authorization/sigv4.hpp"
#include "storage/iceberg_table_information.hpp"
#include "storage/authorization/oauth2.hpp"
#include "storage/irc_schema_entry.hpp"
#include "metadata/iceberg_partition_spec.hpp"

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/common/enums/http_status_code.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

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
	auto table_key = table.GetTableKey();
	ic_catalog.StoreLoadTableResult(table_key, std::move(get_table_result.result_));
	auto &cached_table_result = ic_catalog.GetLoadTableResult(table_key);
	table.table_metadata = IcebergTableMetadata::FromLoadTableResult(*cached_table_result.load_table_result);
	auto &schemas = table.table_metadata.schemas;

	//! It should be impossible to have a metadata file without any schema
	D_ASSERT(!schemas.empty());
	for (auto &table_schema : schemas) {
		table.CreateSchemaVersion(*table_schema.second);
	}
	return true;
}

void ICTableSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	lock_guard<mutex> lock(entry_lock);
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

const case_insensitive_map_t<IcebergTableInformation> &ICTableSet::GetEntries() {
	return entries;
}

case_insensitive_map_t<IcebergTableInformation> &ICTableSet::GetEntriesMutable() {
	return entries;
}

void ICTableSet::LoadEntries(ClientContext &context) {
	auto &irc_transaction = IRCTransaction::Get(context, catalog);
	bool schema_listed = irc_transaction.listed_schemas.find(schema.name) != irc_transaction.listed_schemas.end();
	if (schema_listed) {
		return;
	}
	auto &ic_catalog = catalog.Cast<IRCatalog>();
	auto tables = IRCAPI::GetTables(context, ic_catalog, schema);
	for (auto &table : tables) {
		entries.emplace(table.name, IcebergTableInformation(ic_catalog, schema, table.name));
	}
	irc_transaction.listed_schemas.insert(schema.name);
}

bool ICTableSet::CreateNewEntry(ClientContext &context, IRCatalog &catalog, IRCSchemaEntry &schema,
                                CreateTableInfo &info) {
	auto table_name = info.table;
	auto &irc_transaction = IRCTransaction::Get(context, catalog);

	auto key = IcebergTableInformation::GetTableKey(schema.namespace_items, info.table);
	irc_transaction.updated_tables.emplace(key, IcebergTableInformation(catalog, schema, info.table));
	D_ASSERT(irc_transaction.updated_tables.count(key) > 0);
	auto &table_info = irc_transaction.updated_tables.find(key)->second;
	auto table_entry = make_uniq<ICTableEntry>(table_info, catalog, schema, info);
	auto table_ptr = table_entry.get();
	table_entry->table_info.schema_versions[0] = std::move(table_entry);
	table_ptr->table_info.table_metadata.schemas[0] = IcebergCreateTableRequest::CreateIcebergSchema(table_ptr);
	table_ptr->table_info.table_metadata.current_schema_id = 0;
	table_ptr->table_info.table_metadata.schemas[0]->schema_id = 0;

	// Immediately create the table with stage_create = true to get metadata & data location(s)
	// transaction commit will either commit with data (OR) create the table with stage_create = false
	auto load_table_result =
	    make_uniq<const rest_api_objects::LoadTableResult>(IRCAPI::CommitNewTable(context, catalog, table_ptr));

	catalog.StoreLoadTableResult(key, std::move(load_table_result));
	auto &cached_table_result = catalog.GetLoadTableResult(key);

	table_ptr->table_info.table_metadata =
	    IcebergTableMetadata::FromTableMetadata(cached_table_result.load_table_result->metadata);

	// if we stage created the table, we add an assert create
	if (catalog.attach_options.supports_stage_create) {
		table_info.AddAssertCreate(irc_transaction);
	}
	// other required updates to the table
	table_info.AddAssignUUID(irc_transaction);
	table_info.AddUpradeFormatVersion(irc_transaction);
	table_info.AddSchema(irc_transaction);
	table_info.AddSetCurrentSchema(irc_transaction);
	table_info.AddPartitionSpec(irc_transaction);
	table_info.SetDefaultSpec(irc_transaction);
	table_info.AddSortOrder(irc_transaction);
	table_info.SetDefaultSortOrder(irc_transaction);
	table_info.SetLocation(irc_transaction);
	return true;
}

unique_ptr<ICTableInfo> ICTableSet::GetTableInfo(ClientContext &context, IRCSchemaEntry &schema,
                                                 const string &table_name) {
	throw NotImplementedException("ICTableSet::GetTableInfo");
}

optional_ptr<CatalogEntry> ICTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	lock_guard<mutex> l(entry_lock);
	auto &ic_catalog = catalog.Cast<IRCatalog>();
	auto &irc_transaction = IRCTransaction::Get(context, catalog);
	const auto table_name = lookup.GetEntryName();
	// first check transaction entries
	auto table_key = IcebergTableInformation::GetTableKey(schema.namespace_items, table_name);
	// Check if table has been deleted within in the transaction.
	if (irc_transaction.deleted_tables.count(table_key) > 0) {
		return nullptr;
	}
	// Check if the table has been updated within the transaction
	auto transaction_entry = irc_transaction.updated_tables.find(table_key);
	if (transaction_entry != irc_transaction.updated_tables.end()) {
		return transaction_entry->second.GetSchemaVersion(lookup.GetAtClause());
	}
	auto previous_requested_snapshot = irc_transaction.requested_tables.find(table_name);
	if (previous_requested_snapshot != irc_transaction.requested_tables.end()) {
		// transaction has already looked up this table, find it in entries
		auto entry = entries.find(table_name);
		if (entry == entries.end()) {
			// table no longer exists (was most likely dropped in another transaction)
			// TODO: we can recreate the table (but not insert it in the ICTableSet) by pulling it back
			//  out of the MetadataCache. This doesn't make sense in the long run, as the transaction
			//  will fail regardless
			return nullptr;
		}
		return entry->second.GetSchemaVersion(lookup.GetAtClause());
	}

	if (!IRCAPI::VerifyTableExistence(context, ic_catalog, schema, table_name)) {
		return nullptr;
	}
	if (entries.find(table_name) != entries.end()) {
		entries.erase(table_name);
	}
	auto it = entries.emplace(table_name, IcebergTableInformation(ic_catalog, schema, table_name));
	auto entry = it.first;
	FillEntry(context, entry->second);
	auto ret = entry->second.GetSchemaVersion(lookup.GetAtClause());

	// get the latest information and save it to the transaction cache
	auto &ic_ret = ret->Cast<ICTableEntry>();
	auto latest_snapshot = ic_ret.table_info.table_metadata.GetLatestSnapshot();
	idx_t latest_sequence_number, latest_snapshot_id;
	if (latest_snapshot) {
		latest_snapshot_id = latest_snapshot->snapshot_id;
		latest_sequence_number = latest_snapshot->sequence_number;
	} else {
		// table is not yet initialized.
		latest_sequence_number = 0;
		latest_snapshot_id = -1;
	}

	irc_transaction.requested_tables.emplace(table_name, TableInfoCache(latest_sequence_number, latest_snapshot_id));
	return ret;
}

} // namespace duckdb
