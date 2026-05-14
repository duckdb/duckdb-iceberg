#include "catalog/rest/iceberg_table_set.hpp"

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/common/enums/http_status_code.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/planner/expression_binder/table_function_binder.hpp"

#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "common/iceberg_constants.hpp"
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
		lock_guard<mutex> cache_lock(ic_catalog.table_request_cache.Lock());
		auto cached_result = ic_catalog.table_request_cache.Get(context, table_key, cache_lock);
		if (cached_result) {
			// Use the cached result instead of making a new request
			table.InitializeFromLoadTableResult(*cached_result->load_table_result);
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
	ic_catalog.table_request_cache.SetOrOverwrite(context, table_key, std::move(get_table_result.result_));
	{
		lock_guard<std::mutex> cache_lock(ic_catalog.table_request_cache.Lock());
		auto cached_table_result = ic_catalog.table_request_cache.Get(context, table_key, cache_lock, false);
		D_ASSERT(cached_table_result);
		auto &load_table_result = *cached_table_result->load_table_result;
		table.InitializeFromLoadTableResult(load_table_result);
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

mutex &IcebergTableSet::GetEntryLock() {
	return entry_lock;
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

shared_ptr<IcebergTableInformation>
IcebergTableSet::CreateEntryInternal(lock_guard<mutex> &guard, const string &name, IcebergTableInformation &&table,
                                     shared_ptr<IcebergTableInformation> &old_entry) {
	auto it = entries.find(name);
	if (it != entries.end()) {
		old_entry = std::move(it->second);
		it->second = make_shared_ptr<IcebergTableInformation>(std::move(table));
	} else {
		it = entries.emplace(name, make_shared_ptr<IcebergTableInformation>(std::move(table))).first;
	}
	return it->second;
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
	auto &result_schema = table_metadata.AddSchemaOrGetExisting(std::move(new_schema));
	if (result_schema.schema_id != 0) {
		throw InternalException("Adding initial schema didn't result in schema id 0? (actual: %d)",
		                        result_schema.schema_id);
	}
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

	catalog.table_request_cache.SetOrOverwrite(context, key, std::move(load_table_result));
	{
		lock_guard<mutex> cache_lock(catalog.table_request_cache.Lock());
		auto cached_table_result = catalog.table_request_cache.Get(context, key, cache_lock, false);
		D_ASSERT(cached_table_result);
		auto &load_table_result = cached_table_result->load_table_result;
		table_info.InitializeFromLoadTableResult(*load_table_result, false);
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

	iceberg_transaction.SetLatestTableState(table_info, IcebergTableStatus::ALIVE);
	return table_info;
}

optional_ptr<CatalogEntry> IcebergTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	lock_guard<mutex> l(entry_lock);
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);
	const auto &table_name = lookup.GetEntryName();
	// first check transaction entries
	const auto table_key = IcebergTableInformation::GetTableKey(schema.namespace_items, table_name);
	auto latest_state = iceberg_transaction.GetLatestTableState(table_key);

	auto at = lookup.GetAtClause();
	if (latest_state) {
		if (!latest_state->IsAlive()) {
			// If table has been deleted or is missing within the transaction, return null
			return nullptr;
		}
		auto &table_info = latest_state->GetInfo();
		return table_info.GetSchemaVersion(context, at);
	}

	//! Preserve the old version in case our replacement fails
	shared_ptr<IcebergTableInformation> old_version;
	auto new_version =
	    CreateEntryInternal(l, table_name, IcebergTableInformation(ic_catalog, schema, table_name), old_version);
	auto &table_info = *new_version;
	if (!FillEntry(context, table_info)) {
		if (old_version) {
			entries[table_name] = std::move(old_version);
		} else {
			entries.erase(table_name);
		}
		//! The table doesn't exist in the catalog
		iceberg_transaction.SetLatestTableState(table_key, IcebergTableStatus::MISSING);
		return nullptr;
	}

	iceberg_transaction.tables[table_key] = new_version;
	auto ret = table_info.GetSchemaVersion(context, at);
	if (!ret) {
		return nullptr;
	}

	// get the latest information and save it to the transaction cache
	auto &ic_ret = ret->Cast<IcebergTableEntry>();
	auto latest_snapshot = ic_ret.table_info.table_metadata.GetLatestSnapshot();

	// Log warning on schema_id mismatch
	auto &meta_transaction = MetaTransaction::Get(context);
	auto transaction_start = meta_transaction.GetCurrentTransactionStartTimestamp();
	auto transaction_start_millis = Timestamp::GetEpochMs(transaction_start);

	auto &table_metadata_last_updated_at = ic_ret.table_info.table_metadata.last_updated_ms;

	if (transaction_start_millis < table_metadata_last_updated_at.value &&
	    (!latest_snapshot || latest_snapshot->GetSchemaId() != ic_ret.table_info.table_metadata.GetCurrentSchemaId())) {
		DUCKDB_LOG_WARNING(
		    context, "Detected schema change during transaction (schema_id mismatch); ACID guarantees may not hold.");
	}

	iceberg_transaction.SetLatestTableState(table_info, IcebergTableStatus::ALIVE);
	return ret;
}

// ─── View operations ─────────────────────────────────────────────────────────

const case_insensitive_map_t<unique_ptr<CreateViewInfo>> &IcebergTableSet::GetViewEntries() const {
	return view_entries;
}

case_insensitive_map_t<unique_ptr<CreateViewInfo>> &IcebergTableSet::GetViewEntriesMutable() {
	return view_entries;
}

void IcebergTableSet::InvalidateViewCache(const string &view_name) {
	view_entries.erase(view_name);
	view_catalog_entries.erase(view_name);
}

void IcebergTableSet::LoadViewEntries(ClientContext &context) {
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);
	bool views_listed =
	    iceberg_transaction.listed_view_schemas.find(schema.name) != iceberg_transaction.listed_view_schemas.end();
	if (views_listed) {
		return;
	}
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	// Only attempt to list views if the endpoint is supported
	if (ic_catalog.supported_urls.find("GET /v1/{prefix}/namespaces/{namespace}/views") ==
	    ic_catalog.supported_urls.end()) {
		iceberg_transaction.listed_view_schemas.insert(schema.name);
		return;
	}
	auto views = IRCAPI::GetViews(context, ic_catalog, schema);
	for (auto &view : views) {
		if (view_entries.find(view.name) == view_entries.end()) {
			auto info = make_uniq<CreateViewInfo>(schema, view.name);
			view_entries.emplace(view.name, std::move(info));
		}
	}
	iceberg_transaction.listed_view_schemas.insert(schema.name);
}

optional_ptr<CatalogEntry> IcebergTableSet::GetViewEntry(ClientContext &context, const string &view_name) {
	lock_guard<mutex> l(entry_lock);
	return GetViewEntryInternal(context, view_name);
}

optional_ptr<CatalogEntry> IcebergTableSet::GetViewEntryInternal(ClientContext &context, const string &view_name) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);

	// Check if view was deleted in this transaction
	auto view_key = IcebergTableInformation::GetTableKey(schema.namespace_items, view_name);
	if (iceberg_transaction.deleted_views.count(view_key) > 0) {
		return nullptr;
	}

	// Check if we already have a cached ViewCatalogEntry for this view
	auto cached_it = view_catalog_entries.find(view_name);
	if (cached_it != view_catalog_entries.end()) {
		return cached_it->second.get();
	}

	// Check if the view was created in this transaction
	auto created_it = iceberg_transaction.created_views.find(view_key);
	if (created_it != iceberg_transaction.created_views.end()) {
		auto &view_info = created_it->second;
		auto view_entry = make_uniq<ViewCatalogEntry>(catalog, schema, *view_info);
		auto result = view_entry.get();
		view_catalog_entries.emplace(view_name, std::move(view_entry));
		return result;
	}

	// Check if the view endpoint is supported
	if (ic_catalog.supported_urls.find("GET /v1/{prefix}/namespaces/{namespace}/views/{view}") ==
	    ic_catalog.supported_urls.end()) {
		return nullptr;
	}

	// Load the view from the REST catalog
	rest_api_objects::LoadViewResult load_result;
	try {
		load_result = IRCAPI::GetView(context, ic_catalog, schema, view_name);
	} catch (...) {
		return nullptr;
	}

	// Find a SQL representation with the DuckDB dialect first, fall back to any SQL representation.
	string view_sql;
	auto &metadata = load_result.metadata;
	// Find the current version
	for (auto &version : metadata.versions) {
		if (version.version_id != metadata.current_version_id) {
			continue;
		}
		// Search representations for DuckDB dialect first
		for (auto &repr : version.representations) {
			if (!repr.has_sqlview_representation) {
				continue;
			}
			if (repr.sqlview_representation.dialect == IcebergConstants::ViewDuckDBDialect) {
				view_sql = repr.sqlview_representation.sql;
				break;
			}
		}
		// If no DuckDB dialect found, use the first available SQL representation
		if (view_sql.empty()) {
			for (auto &repr : version.representations) {
				if (repr.has_sqlview_representation) {
					DUCKDB_LOG_WARNING(context,
					                   "View '%s' has no representation with dialect '%s'; falling back to "
					                   "dialect '%s' (the SQL may not parse cleanly in DuckDB)",
					                   view_name, IcebergConstants::ViewDuckDBDialect,
					                   repr.sqlview_representation.dialect);
					view_sql = repr.sqlview_representation.sql;
					break;
				}
			}
		}
		break;
	}

	if (view_sql.empty()) {
		// View exists but has no SQL — create an unbound placeholder so it's visible in catalog
		// but fails with a clear error when someone tries to query it
		view_sql = StringUtil::Format("SELECT error('View \"%s\" exists in the Iceberg catalog but has no SQL "
		                              "representation that DuckDB can use')",
		                              view_name);
	}

	// Parse the SQL text into a SelectStatement
	unique_ptr<SelectStatement> view_query;
	try {
		view_query = CreateViewInfo::ParseSelect(view_sql);
	} catch (std::exception &) {
		// View exists but SQL can't be parsed (e.g., Spark-only syntax) — create a placeholder
		// that will produce a clear error when queried
		auto error_sql = StringUtil::Format(
		    "SELECT error('View \"%s\" exists in the Iceberg catalog but its SQL dialect cannot be parsed by DuckDB')",
		    view_name);
		view_query = CreateViewInfo::ParseSelect(error_sql);
	}

	auto view_info = make_uniq<CreateViewInfo>(schema, view_name);
	view_info->query = std::move(view_query);
	view_info->sql = view_sql;

	auto view_entry = make_uniq<ViewCatalogEntry>(catalog, schema, *view_info);
	auto result = view_entry.get();
	view_catalog_entries.emplace(view_name, std::move(view_entry));
	return result;
}

void IcebergTableSet::ScanViews(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	lock_guard<mutex> lock(entry_lock);
	LoadViewEntries(context);
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);

	// Include views created in this transaction
	for (auto &created_view : iceberg_transaction.created_views) {
		auto &view_info = created_view.second;
		auto cached_it = view_catalog_entries.find(view_info->view_name);
		if (cached_it == view_catalog_entries.end()) {
			auto view_entry = make_uniq<ViewCatalogEntry>(catalog, schema, *view_info);
			view_catalog_entries.emplace(view_info->view_name, std::move(view_entry));
		}
	}

	for (auto &view_entry_kv : view_entries) {
		auto &view_name = view_entry_kv.first;
		// Skip views deleted in this transaction
		auto view_key = IcebergTableInformation::GetTableKey(schema.namespace_items, view_name);
		if (iceberg_transaction.deleted_views.count(view_key) > 0) {
			continue;
		}
		// Get or create the ViewCatalogEntry
		auto cached_it = view_catalog_entries.find(view_name);
		if (cached_it != view_catalog_entries.end()) {
			callback(*cached_it->second);
		} else {
			auto entry = GetViewEntryInternal(context, view_name);
			if (entry) {
				callback(*entry);
			}
		}
	}

	// Also scan views created in this transaction that aren't in view_entries yet
	for (auto &created_view : iceberg_transaction.created_views) {
		auto &view_name = created_view.second->view_name;
		if (view_entries.find(view_name) != view_entries.end()) {
			continue; // already scanned above
		}
		auto cached_it = view_catalog_entries.find(view_name);
		if (cached_it != view_catalog_entries.end()) {
			callback(*cached_it->second);
		}
	}
}

} // namespace duckdb
