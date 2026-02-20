#include "catalog_api.hpp"
#include "catalog_utils.hpp"
#include "iceberg_logging.hpp"

#include "storage/catalog/iceberg_catalog.hpp"
#include "storage/catalog/iceberg_table_set.hpp"

#include "storage/catalog/iceberg_table_entry.hpp"
#include "storage/iceberg_transaction.hpp"
#include "storage/authorization/sigv4.hpp"
#include "storage/iceberg_table_information.hpp"
#include "storage/authorization/oauth2.hpp"
#include "storage/catalog/iceberg_schema_entry.hpp"
#include "metadata/iceberg_partition_spec.hpp"

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/common/enums/http_status_code.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/planner/expression_binder/table_function_binder.hpp"

namespace duckdb {

IcebergTableSet::IcebergTableSet(IcebergSchemaEntry &schema) : schema(schema), catalog(schema.ParentCatalog()) {
}

bool IcebergTableSet::FillEntry(ClientContext &context, IcebergTableInformation &table) {
	if (!table.schema_versions.empty()) {
		return true;
	}

	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto table_key = table.GetTableKey();

	// Only check cache if MAX_TABLE_STALENESS option is set
	if (ic_catalog.attach_options.max_table_staleness_micros.IsValid()) {
		lock_guard<std::mutex> cache_lock(ic_catalog.GetMetadataCacheLock());
		auto cached_result = ic_catalog.TryGetValidCachedLoadTableResult(table_key, cache_lock);
		if (cached_result) {
			// Use the cached result instead of making a new request
			table.table_metadata = IcebergTableMetadata::FromLoadTableResult(*cached_result->load_table_result);
			auto &schemas = table.table_metadata.schemas;
			D_ASSERT(!schemas.empty());
			for (auto &table_schema : schemas) {
				table.CreateSchemaVersion(*table_schema.second);
			}
			return true;
		}
	}

	// No valid cached result or caching disabled, make a new request
	auto get_table_result = IRCAPI::GetTable(context, ic_catalog, schema, table.name);
	if (get_table_result.has_error) {
		if (get_table_result.error_._error.type == "NoSuchIcebergTableException") {
			return false;
		}
		if (get_table_result.status_ == HTTPStatusCode::Forbidden_403 ||
		    get_table_result.status_ == HTTPStatusCode::Unauthorized_401 ||
		    get_table_result.status_ == HTTPStatusCode::NotFound_404) {
			return false;
		}
		throw HTTPException(get_table_result.error_._error.message);
	}
	ic_catalog.StoreLoadTableResult(table_key, std::move(get_table_result.result_));
	{
		lock_guard<std::mutex> cache_lock(ic_catalog.GetMetadataCacheLock());
		auto cached_table_result = ic_catalog.TryGetValidCachedLoadTableResult(table_key, cache_lock, false);
		D_ASSERT(cached_table_result);
		auto &load_table_result = *cached_table_result->load_table_result;
		table.table_metadata = IcebergTableMetadata::FromLoadTableResult(load_table_result);
	}
	auto &schemas = table.table_metadata.schemas;

	//! It should be impossible to have a metadata file without any schema
	D_ASSERT(!schemas.empty());
	for (auto &table_schema : schemas) {
		table.CreateSchemaVersion(*table_schema.second);
	}
	return true;
}

void IcebergTableSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
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
		auto table_entry = make_uniq<IcebergTableEntry>(table_info, catalog, schema, info);
		if (!table_entry->internal) {
			table_entry->internal = schema.internal;
		}
		auto result = table_entry.get();
		if (result->name.empty()) {
			throw InternalException("IcebergTableSet::CreateEntry called with empty name");
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

const case_insensitive_map_t<IcebergTableInformation> &IcebergTableSet::GetEntries() {
	return entries;
}

case_insensitive_map_t<IcebergTableInformation> &IcebergTableSet::GetEntriesMutable() {
	return entries;
}

void IcebergTableSet::LoadEntries(ClientContext &context) {
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);
	bool schema_listed =
	    iceberg_transaction.listed_schemas.find(schema.name) != iceberg_transaction.listed_schemas.end();
	if (schema_listed) {
		return;
	}
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto tables = IRCAPI::GetTables(context, ic_catalog, schema);
	for (auto &table : tables) {
		entries.emplace(table.name, IcebergTableInformation(ic_catalog, schema, table.name));
	}
	iceberg_transaction.listed_schemas.insert(schema.name);
}

static Value ParseFormatVersionProperty(TableFunctionBinder &binder, ClientContext &context,
                                        const ParsedExpression &expr_ref, string property_name, LogicalType type) {
	auto expr = expr_ref.Copy();
	auto bound_expr = binder.Bind(expr);
	if (bound_expr->HasParameter()) {
		throw ParameterNotResolvedException();
	}

	auto val = ExpressionExecutor::EvaluateScalar(context, *bound_expr, true);
	if (val.IsNull()) {
		throw BinderException("NULL is not supported as a valid option for '%s'", property_name);
	}
	if (!val.DefaultTryCastAs(type, true)) {
		throw InvalidInputException("Can't cast '%s' property (%s) to %s", property_name, val.ToString(),
		                            type.ToString());
	}
	return val;
}

bool IcebergTableSet::CreateNewEntry(ClientContext &context, IcebergCatalog &catalog, IcebergSchemaEntry &schema,
                                     CreateTableInfo &info) {
	auto table_name = info.table;
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);

	auto binder = Binder::CreateBinder(context);
	TableFunctionBinder property_binder(*binder, context, "format-version");

	optional_idx iceberg_version;
	case_insensitive_map_t<Value> table_properties;
	// format version must be verified
	auto format_version_it = info.options.find("format-version");
	if (format_version_it != info.options.end()) {
		iceberg_version = ParseFormatVersionProperty(property_binder, context, *format_version_it->second,
		                                             "format-version", LogicalType::INTEGER)
		                      .GetValue<int32_t>();
		if (iceberg_version.GetIndex() < 1) {
			throw InvalidInputException("The lowest supported iceberg version is 1!");
		}
	} else {
		iceberg_version = 2;
	}

	string location;
	auto location_it = info.options.find("location");
	if (location_it != info.options.end()) {
		location =
		    ParseFormatVersionProperty(property_binder, context, *location_it->second, "location", LogicalType::VARCHAR)
		        .GetValue<string>();
	}

	auto key = IcebergTableInformation::GetTableKey(schema.namespace_items, info.table);
	iceberg_transaction.updated_tables.emplace(key, IcebergTableInformation(catalog, schema, info.table));
	D_ASSERT(iceberg_transaction.updated_tables.count(key) > 0);
	auto &table_info = iceberg_transaction.updated_tables.find(key)->second;
	auto table_entry = make_uniq<IcebergTableEntry>(table_info, catalog, schema, info);
	auto table_ptr = table_entry.get();
	table_entry->table_info.schema_versions[0] = std::move(table_entry);
	table_ptr->table_info.table_metadata.schemas[0] = IcebergCreateTableRequest::CreateIcebergSchema(table_ptr);
	table_ptr->table_info.table_metadata.current_schema_id = 0;
	table_ptr->table_info.table_metadata.schemas[0]->schema_id = 0;
	table_ptr->table_info.table_metadata.iceberg_version = iceberg_version.GetIndex();

	// Get Location
	if (!location.empty()) {
		table_ptr->table_info.table_metadata.location = location;
	}
	for (auto &option : info.options) {
		if (option.first == "format-version" || option.first == "location") {
			continue;
		}
		auto option_val =
		    ParseFormatVersionProperty(property_binder, context, *option.second, option.first, LogicalType::VARCHAR)
		        .GetValue<string>();
		table_ptr->table_info.table_metadata.table_properties.emplace(option.first, option_val);
	}

	// Immediately create the table with stage_create = true to get metadata & data location(s)
	// transaction commit will either commit with data (OR) create the table with stage_create = false
	auto load_table_result =
	    make_uniq<const rest_api_objects::LoadTableResult>(IRCAPI::CommitNewTable(context, catalog, table_ptr));

	catalog.StoreLoadTableResult(key, std::move(load_table_result));
	{
		lock_guard<std::mutex> cache_lock(catalog.GetMetadataCacheLock());
		auto cached_table_result = catalog.TryGetValidCachedLoadTableResult(key, cache_lock, false);
		D_ASSERT(cached_table_result);
		auto &load_table_result = cached_table_result->load_table_result;
		table_ptr->table_info.table_metadata = IcebergTableMetadata::FromTableMetadata(load_table_result->metadata);
	}

	// if we stage created the table, we add an assert create
	if (catalog.attach_options.supports_stage_create) {
		table_info.AddAssertCreate(iceberg_transaction);
	}
	// other required updates to the table
	table_info.AddAssignUUID(iceberg_transaction);
	table_info.AddUpradeFormatVersion(iceberg_transaction);
	table_info.AddSchema(iceberg_transaction);
	table_info.AddSetCurrentSchema(iceberg_transaction);
	table_info.AddPartitionSpec(iceberg_transaction);
	table_info.SetDefaultSpec(iceberg_transaction);
	table_info.AddSortOrder(iceberg_transaction);
	table_info.SetDefaultSortOrder(iceberg_transaction);
	table_info.SetLocation(iceberg_transaction);
	table_info.SetProperties(iceberg_transaction, table_info.table_metadata.table_properties);
	return true;
}

optional_ptr<CatalogEntry> IcebergTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	lock_guard<mutex> l(entry_lock);
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);
	const auto table_name = lookup.GetEntryName();
	// first check transaction entries
	auto table_key = IcebergTableInformation::GetTableKey(schema.namespace_items, table_name);
	// Check if table has been deleted within in the transaction.
	if (iceberg_transaction.deleted_tables.count(table_key) > 0) {
		return nullptr;
	}
	// Check if the table has been updated within the transaction
	auto transaction_entry = iceberg_transaction.updated_tables.find(table_key);
	if (transaction_entry != iceberg_transaction.updated_tables.end()) {
		return transaction_entry->second.GetSchemaVersion(lookup.GetAtClause());
	}
	auto previous_request_info = iceberg_transaction.GetTableRequestResult(table_key);
	if (previous_request_info.exists) {
		// transaction has already looked up this table, find it in entries
		auto entry = entries.find(table_name);
		if (entry == entries.end()) {
			// table no longer exists (was most likely dropped in another transaction)
			// TODO: we can recreate the table (but not insert it in the IcebergTableSet) by pulling it back
			//  out of the MetadataCache. This doesn't make sense in the long run, as the transaction
			//  will fail regardless
			return nullptr;
		}
		return entry->second.GetSchemaVersion(lookup.GetAtClause());
	}

	if (entries.find(table_name) != entries.end()) {
		entries.erase(table_name);
	}
	auto it = entries.emplace(table_name, IcebergTableInformation(ic_catalog, schema, table_name));
	auto entry = it.first;
	if (!FillEntry(context, entry->second)) {
		// Table doesn't exist
		entries.erase(entry);
		iceberg_transaction.RecordTableRequest(table_key);
		return nullptr;
	}
	auto ret = entry->second.GetSchemaVersion(lookup.GetAtClause());

	// get the latest information and save it to the transaction cache
	auto &ic_ret = ret->Cast<IcebergTableEntry>();
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

	iceberg_transaction.RecordTableRequest(table_key, latest_sequence_number, latest_snapshot_id);
	return ret;
}

} // namespace duckdb
