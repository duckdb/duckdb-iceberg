#include "catalog/rest/iceberg_table_set.hpp"

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/common/enums/http_status_code.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/planner/expression_binder/table_function_binder.hpp"

#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "iceberg_logging.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/storage/authorization/sigv4.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/storage/authorization/oauth2.hpp"
#include "catalog/rest/catalog_entry/iceberg_schema_entry.hpp"
#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "catalog/rest/transaction/iceberg_transaction_update.hpp"

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
			auto &schemas = table.table_metadata.GetSchemas();
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
		if (get_table_result.status_ == HTTPStatusCode::NotFound_404) {
			// Glue returns 404 when a table is not an Iceberg Table with the error message
			// "input table is not an iceberg table" of type "NoSuchIcebergTableException"
			// Otherwise the error is a standard 404, we return false and duckdb will return
			// that the table does not exist.
			// see test/sql/cloud/test_glue_catalog_with_other_tables.test for testing
			if (get_table_result.error_._error.type != "NoSuchIcebergTableException") {
				return false;
			}
		}
		// surface all other errror messages. Not found will be returned as a catalog exception
		// User should not if they do not have permission or if they are not authorized (or 500)
		throw HTTPException(
		    StringUtil::Format("GetTableInformation endpoint returned response code %s with message \"%s\"",
		                       EnumUtil::ToString(get_table_result.status_), get_table_result.error_._error.message));
	}
	ic_catalog.StoreLoadTableResult(table_key, std::move(get_table_result.result_));
	{
		lock_guard<std::mutex> cache_lock(ic_catalog.GetMetadataCacheLock());
		auto cached_table_result = ic_catalog.TryGetValidCachedLoadTableResult(table_key, cache_lock, false);
		D_ASSERT(cached_table_result);
		auto &load_table_result = *cached_table_result->load_table_result;
		table.table_metadata = IcebergTableMetadata::FromLoadTableResult(load_table_result);
	}
	auto &schemas = table.table_metadata.GetSchemas();

	//! It should be impossible to have a metadata file without any schema
	D_ASSERT(!schemas.empty());
	for (auto &table_schema : schemas) {
		table.CreateSchemaVersion(*table_schema.second);
	}
	return true;
}

void IcebergTableSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	lock_guard<mutex> lock(entry_lock);
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);
	LoadEntries(context);
	case_insensitive_set_t non_iceberg_tables;
	auto schema_component = IRCPathComponent::NamespaceComponent(schema.namespace_items);
	auto table_namespace = schema_component.encoded;
	for (auto &entry : entries) {
		auto &table_info = *entry.second;
		auto table_key = table_info.GetTableKey();
		iceberg_transaction.tables[table_key] = entry.second;

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
		auto table_entry = make_uniq<IcebergTableEntry>(table_info, catalog, schema, info, optional_idx());
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

const case_insensitive_map_t<shared_ptr<IcebergTableInformation>> &IcebergTableSet::GetEntries() {
	return entries;
}

case_insensitive_map_t<shared_ptr<IcebergTableInformation>> &IcebergTableSet::GetEntriesMutable() {
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
		entries.emplace(table.name, make_shared_ptr<IcebergTableInformation>(ic_catalog, schema, table.name));
	}
	iceberg_transaction.listed_schemas.insert(schema.name);
}

static Value ParseTableProperty(TableFunctionBinder &binder, ClientContext &context, const ParsedExpression &expr_ref,
                                const string &property_name, const LogicalType &type) {
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

IcebergTableInformation &IcebergTableSet::CreateNewEntry(ClientContext &context, IcebergCatalog &catalog,
                                                         IcebergSchemaEntry &schema, CreateTableInfo &info) {
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);

	auto binder = Binder::CreateBinder(context);
	TableFunctionBinder property_binder(*binder, context, "format-version");

	optional_idx iceberg_version;
	case_insensitive_map_t<Value> table_properties;
	// format version must be verified
	auto format_version_it = info.options.find("format-version");
	if (format_version_it != info.options.end()) {
		iceberg_version = ParseTableProperty(property_binder, context, *format_version_it->second, "format-version",
		                                     LogicalType::INTEGER)
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
		location = ParseTableProperty(property_binder, context, *location_it->second, "location", LogicalType::VARCHAR)
		               .GetValue<string>();
	}

	auto key = IcebergTableInformation::GetTableKey(schema.namespace_items, info.table);
	auto &alter_update = iceberg_transaction.GetOrCreateAlter();
	auto &table_info = alter_update.CreateTable(key, IcebergTableInformation(catalog, schema, info.table));
	// auto &table_info = emplace_res.first->second;
	auto &table_metadata = table_info.table_metadata;
	auto table_entry = make_uniq<IcebergTableEntry>(table_info, catalog, schema, info, 0);
	auto table_ptr = table_entry.get();
	table_info.schema_versions[0] = std::move(table_entry);
	table_metadata.iceberg_version = iceberg_version.GetIndex();
	int32_t last_column_id;

	auto new_schema = IcebergCreateTableRequest::CreateIcebergSchema(context, table_metadata, table_ptr->GetColumns(),
	                                                                 table_ptr->GetConstraints(), last_column_id);
	new_schema->schema_id = 0;
	table_metadata.AddSchema(std::move(new_schema));
	table_metadata.SetCurrentSchemaId(0);
	table_metadata.last_column_id = last_column_id;

	// Get Location
	if (!location.empty()) {
		table_metadata.location = location;
	}
	for (auto &option : info.options) {
		if (option.first == "format-version" || option.first == "location") {
			continue;
		}
		auto option_val =
		    ParseTableProperty(property_binder, context, *option.second, option.first, LogicalType::VARCHAR)
		        .GetValue<string>();
		table_metadata.table_properties.emplace(option.first, option_val);
	}

	auto &current_schema = table_info.table_metadata.GetLatestSchema();
	table_ptr->table_info.table_metadata.default_spec_id = 0;
	table_ptr->table_info.SetPartitionedBy(iceberg_transaction, info.partition_keys, current_schema, true);

	// Immediately create the table with stage_create = true to get metadata & data location(s)
	// transaction commit will either commit with data (OR) create the table with stage_create = false
	auto load_table_result =
	    make_uniq<const rest_api_objects::LoadTableResult>(IRCAPI::CommitNewTable(context, catalog, *table_ptr));

	catalog.StoreLoadTableResult(key, std::move(load_table_result));
	{
		lock_guard<std::mutex> cache_lock(catalog.GetMetadataCacheLock());
		auto cached_table_result = catalog.TryGetValidCachedLoadTableResult(key, cache_lock, false);
		D_ASSERT(cached_table_result);
		auto &load_table_result = cached_table_result->load_table_result;
		table_metadata = IcebergTableMetadata::FromTableMetadata(load_table_result->metadata);
	}

	// if we stage created the table, we add an assert create
	auto &transaction_data = table_info.GetOrCreateTransactionData(iceberg_transaction);
	if (catalog.attach_options.supports_stage_create) {
		transaction_data.TableAddAssertCreate();
	}
	// other required updates to the table
	transaction_data.TableAssignUUID();
	transaction_data.TableAddUpradeFormatVersion();
	transaction_data.TableAddSchema(0);
	transaction_data.TableAddPartitionSpec();
	transaction_data.TableSetDefaultSpec();
	transaction_data.TableAddSortOrder();
	transaction_data.TableSetDefaultSortOrder();
	transaction_data.TableSetLocation();
	transaction_data.TableSetProperties(table_metadata.table_properties);
	return table_info;
}

static IcebergSnapshotLookup GetSnapshotLookup(const IcebergTableInformation &table_info, ClientContext &context,
                                               const EntryLookupInfo &lookup) {
	auto at = lookup.GetAtClause();
	if (!at && !table_info.HasTransactionUpdates()) {
		// if there is no user supplied AT () clause, and the table does not have transaction updates
		// use transaction start time
		return table_info.GetSnapshotLookup(context);
	} else {
		return IcebergSnapshotLookup::FromAtClause(at);
	}
}

optional_ptr<CatalogEntry> IcebergTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	lock_guard<mutex> l(entry_lock);
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);
	const auto &table_name = lookup.GetEntryName();
	// first check transaction entries
	auto table_key = IcebergTableInformation::GetTableKey(schema.namespace_items, table_name);
	auto latest_state = iceberg_transaction.GetLatestTableState(table_key);
	if (latest_state) {
		if (latest_state->status == IcebergTableStatus::DROPPED) {
			// If table has been deleted within the transaction, return null
			return nullptr;
		}
		auto &table_info = latest_state->table.get();
		auto snapshot_lookup = GetSnapshotLookup(table_info, context, lookup);
		return table_info.GetSchemaVersion(snapshot_lookup, context);
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
		auto &table_info = *entry->second;
		auto snapshot_lookup = GetSnapshotLookup(table_info, context, lookup);
		return table_info.GetSchemaVersion(snapshot_lookup, context);
	}

	if (entries.find(table_name) != entries.end()) {
		entries.erase(table_name);
	}
	auto it = entries.emplace(table_name, make_shared_ptr<IcebergTableInformation>(ic_catalog, schema, table_name));
	auto entry = it.first;
	auto &table_info = *entry->second;
	if (!FillEntry(context, table_info)) {
		// Table doesn't exist
		entries.erase(entry);
		iceberg_transaction.RecordTableRequest(table_key);
		return nullptr;
	}

	iceberg_transaction.tables[table_key] = entry->second;
	IcebergSnapshotLookup snapshot_lookup;

	bool is_time_travel = lookup.GetAtClause();
	if (!is_time_travel && !table_info.HasTransactionUpdates()) {
		// if there is no user supplied AT () clause, and the table does not have transaction updates
		// use transaction start time
		snapshot_lookup = table_info.GetSnapshotLookup(context);
	} else {
		auto at = lookup.GetAtClause();
		snapshot_lookup = IcebergSnapshotLookup::FromAtClause(at);
	};

	auto ret = table_info.GetSchemaVersion(snapshot_lookup, context, is_time_travel);

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

	// Log warning on schema_id mismatch
	auto &meta_transaction = MetaTransaction::Get(context);
	auto transaction_start = meta_transaction.GetCurrentTransactionStartTimestamp();
	auto transaction_start_millis = Timestamp::GetEpochMs(transaction_start);

	auto &table_metadata_last_updated_at = ic_ret.table_info.table_metadata.last_updated_ms;

	if (transaction_start_millis < table_metadata_last_updated_at.value &&
	    latest_snapshot->GetSchemaId() != ic_ret.table_info.table_metadata.GetCurrentSchemaId()) {
		DUCKDB_LOG_WARNING(
		    context, "Detected schema change during transaction (schema_id mismatch); ACID guarantees may not hold.");
	}

	iceberg_transaction.RecordTableRequest(table_key, latest_sequence_number, latest_snapshot_id);
	return ret;
}

} // namespace duckdb
