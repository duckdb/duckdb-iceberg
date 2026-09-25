#include "catalog/rest/iceberg_table_set.hpp"
#include "catalog/rest/iceberg_view_entry.hpp"

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
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/deque.hpp"

#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "common/iceberg_constants.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/iceberg_request_executor.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/storage/authorization/sigv4.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/storage/authorization/oauth2.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "catalog/rest/transaction/iceberg_transaction_update.hpp"
#include "iceberg_options.hpp"

namespace duckdb {

IcebergTableSet::IcebergTableSet(IcebergSchemaEntry &schema) : schema(schema), catalog(schema.ParentCatalog()) {
}

bool IcebergTableSet::FillEntry(ClientContext &context, IcebergTable &table) {
	if (TryFillEntryFromCache(context, table)) {
		return true;
	}
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto publication = ic_catalog.table_request_cache.BeginLoad(table.GetTableKey());
	return ApplyLoadResult(table, IRCAPI::GetTable(context, ic_catalog, schema, table.name), *publication);
}

bool IcebergTableSet::TryFillEntryFromCache(ClientContext &context, IcebergTable &table) {
	// If the table is already loaded, no need to fill again
	if (!table.schema_versions.empty()) {
		return true;
	}

	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto table_key = table.GetTableKey();

	// Only check cache if MAX_TABLE_STALENESS option is set
	if (ic_catalog.attach_options.max_table_staleness_micros.IsValid()) {
		auto cache_hit = ic_catalog.table_request_cache.Get(
		    context, table_key, [&](const rest_api_objects::LoadTableResult &cached_result) {
			    // Use the cached result instead of making a new request
			    table.InitializeFromLoadTableResult(cached_result);
		    });
		if (cache_hit) {
			return true;
		}
	}

	return false;
}

bool IcebergTableSet::ApplyLoadResult(IcebergTable &table, IcebergLoadTableResult get_table_result,
                                      LoadTableCachePublication &publication) {
	if (get_table_result.error_) {
		if (get_table_result.status_ == HTTPStatusCode::NotFound_404) {
			// Glue returns 404 when a table is not an Iceberg Table with the error message
			// "input table is not an iceberg table" of type "NoSuchIcebergTableException"
			// Otherwise the error is a standard 404, we return false and duckdb will return
			// that the table does not exist.
			// see test/sql/cloud/test_glue_catalog_with_other_tables.test for testing
			if (get_table_result.error_->_error.type != "NoSuchIcebergTableException") {
				return false;
			}
		}
		// surface all other errror messages. Not found will be returned as a catalog exception
		// User should not if they do not have permission or if they are not authorized (or 500)
		throw HTTPException(
		    StringUtil::Format("GetTableInformation endpoint returned response code %s with message \"%s\"",
		                       EnumUtil::ToString(get_table_result.status_), get_table_result.error_->_error.message));
	}
	auto &load_table_result = *get_table_result.result_;
	table.InitializeFromLoadTableResult(load_table_result);
	// Rejected payloads are destroyed; they must not remain as cache identities on the local table.
	table.initialization_source = nullptr;
	if (publication.TryPublish(std::move(get_table_result.result_))) {
		table.initialization_source = load_table_result;
	}
	return true;
}

namespace {

struct PendingTableLoad {
	explicit PendingTableLoad(IcebergTable &table) : table(table) {
	}

	IcebergTable &table;
	unique_ptr<LoadTableCachePublication> publication;
	shared_ptr<IcebergRequestResult<IcebergLoadTableResult>> result;
};

void WarnTableLoadFailure(ClientContext &context, IcebergTable &table, const ErrorData &error) {
	DUCKDB_LOG_WARNING(context, "Could not resolve the columns of Iceberg table '%s' while listing: %s",
	                   table.GetTableKey(), error.RawMessage());
}

} // namespace

void IcebergTableSet::ScanEagerEntries(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto &transaction = IcebergTransaction::Get(context, catalog);
	auto &scheduler = TaskScheduler::GetScheduler(context);
	// Regular workers can also execute async tasks; NumberOfThreads includes the calling thread.
	const auto window_size = MinValue<idx_t>(8, scheduler.NumberOfThreads() + scheduler.NumberOfAsyncThreads());
	deque<unique_ptr<PendingTableLoad>> pending;
	IcebergRequestExecutor executor(context, ic_catalog);
	auto entry = entries.begin();

	auto schedule_next = [&]() {
		context.InterruptCheck();
		auto &table = *entry->second;
		transaction.tables[table.GetTableKey()] = entry->second;
		++entry;
		auto load = make_uniq<PendingTableLoad>(table);
		bool needs_load = false;
		try {
			needs_load = !TryFillEntryFromCache(context, table);
		} catch (std::exception &ex) {
			context.InterruptCheck();
			WarnTableLoadFailure(context, table, ErrorData(ex));
		}
		if (needs_load) {
			load->publication = ic_catalog.table_request_cache.BeginLoad(table.GetTableKey());
			load->result = executor.Schedule(IcebergLoadTableRequest(table.schema.namespace_items, table.name));
		}
		pending.push_back(std::move(load));
	};

	while (entry != entries.end() && pending.size() < window_size) {
		schedule_next();
	}
	while (!pending.empty()) {
		auto load = std::move(pending.front());
		pending.pop_front();
		if (load->result) {
			try {
				ApplyLoadResult(load->table, executor.WaitAndTakeResult(*load->result), *load->publication);
			} catch (std::exception &ex) {
				ErrorData error(ex);
				if (error.Type() == ExceptionType::INTERRUPT || executor.HasError()) {
					throw;
				}
				context.InterruptCheck();
				WarnTableLoadFailure(context, load->table, error);
			}
		}
		// Refill before invoking the callback, so requests can overlap both consumption and callback work.
		// The task owns its result holder even if this window slot is released before task cleanup finishes.
		if (entry != entries.end()) {
			schedule_next();
		}
		callback(GetScanEntry(load->table));
	}
	// Join at the scan boundary. Exceptions instead cancel and drain through the executor's destructor.
	executor.Drain();
}

CatalogEntry &IcebergTableSet::GetScanEntry(IcebergTable &table_info) const {
	if (!table_info.schema_versions.empty()) {
		// Surface resolved columns, including comments, instead of a listing placeholder.
		auto resolved = table_info.GetLatestSchema();
		if (resolved) {
			return *resolved;
		}
	}
	return GetOrCreateDummy(table_info);
}

IcebergTableSchemaVersion &IcebergTableSet::GetOrCreateDummy(IcebergTable &table_info) const {
	if (table_info.dummy_entry) {
		return *table_info.dummy_entry;
	}
	// create a table entry with fake schema data to avoid calling the LoadTableInformation endpoint for every
	// table while listing schemas
	CreateTableInfo info(schema, Identifier(table_info.name));
	vector<ColumnDefinition> columns;
	auto col = ColumnDefinition(Identifier("__"), LogicalType::UNKNOWN);
	columns.push_back(std::move(col));
	info.columns = ColumnList(std::move(columns));
	auto table_entry = make_uniq<IcebergTableSchemaVersion>(table_info, catalog, schema, info, optional_idx());
	if (!table_entry->internal) {
		table_entry->internal = schema.internal;
	}
	auto result = table_entry.get();
	if (result->name.empty()) {
		throw InternalException("IcebergTableSet::CreateEntry called with empty name");
	}
	table_info.dummy_entry = std::move(table_entry);
	return *table_info.dummy_entry;
}

void IcebergTableSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	annotated_lock_guard<annotated_mutex> lock(entry_lock);
	LoadEntriesInternal(context);
	if (catalog.Cast<IcebergCatalog>().attach_options.table_resolution == IcebergTableResolution::EAGER) {
		ScanEagerEntries(context, callback);
		return;
	}
	auto &transaction = IcebergTransaction::Get(context, catalog);
	for (auto &entry : entries) {
		auto &table = *entry.second;
		transaction.tables[table.GetTableKey()] = entry.second;
		callback(GetScanEntry(table));
	}
}

void IcebergTableSet::ScanTables(ClientContext &context, const std::function<void(IcebergTable &)> &callback) {
	annotated_lock_guard<annotated_mutex> lock(entry_lock);
	LoadEntriesInternal(context);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

void IcebergTableSet::DropEntry(ClientContext &context, DropInfo &info, bool delete_entry) {
	annotated_lock_guard<annotated_mutex> lock(entry_lock);
	auto table_name = info.GetQualifiedName().Name();
	auto entry = entries.find(table_name.GetIdentifierName());
	if (entry == entries.end()) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException("Table %s does not exist", table_name);
	}
	if (info.cascade) {
		throw NotImplementedException("DROP TABLE <table_name> CASCADE is not supported for Iceberg tables currently");
	}
	if (delete_entry) {
		entries.erase(entry);
		return;
	}

	// Add the table to the transaction's deleted tables.
	auto &transaction = IcebergTransaction::Get(context, catalog).Cast<IcebergTransaction>();
	auto &table = transaction.DeleteTable(*entry->second);
	//! FIXME: Schema versions point back to their IcebergTable and must be reinitialized after the copy.
	table.InitSchemaVersions();
}

void IcebergTableSet::RenameEntry(const string &name, const string &new_name, IcebergTable &&new_table) {
	annotated_lock_guard<annotated_mutex> lock(entry_lock);
	auto source = entries.find(name);
	if (source == entries.end()) {
		throw CatalogException("Table %s does not exist", name);
	}
	entries.erase(source);
	shared_ptr<IcebergTable> old_version;
	CreateEntryInternal(new_name, std::move(new_table), old_version);
	if (old_version) {
		throw TransactionException("Table %s was already created by a different transaction!", new_name);
	}
}

void IcebergTableSet::LoadEntriesInternal(ClientContext &context) {
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);
	bool schema_listed = iceberg_transaction.listed_schemas.find(schema.name.GetIdentifierName()) !=
	                     iceberg_transaction.listed_schemas.end();
	if (schema_listed) {
		return;
	}
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	// The request owns its namespace; workers never access or mutate this table set.
	IcebergRequestExecutor executor(context, ic_catalog);
	auto result = executor.Schedule(IcebergListTablesRequest(schema.namespace_items));
	auto tables = executor.WaitAndTakeResult(*result);
	if (context.IsInterrupted()) {
		throw InterruptException();
	}
	ApplyListResult(std::move(tables));
	iceberg_transaction.listed_schemas.insert(schema.name.GetIdentifierName());
}

void IcebergTableSet::ApplyListResult(IcebergListTablesResult tables) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	// A refused listing says nothing about which tables exist, so the cache is left untouched.
	if (tables) {
		case_insensitive_set_t listed;
		for (auto &table : *tables) {
			listed.insert(table.name);
			entries.emplace(table.name, IcebergTable::CreatePlaceholder(ic_catalog, schema, table.name));
		}
		// 'entries' outlives the transaction, so drop the names the listing no longer reports.
		// Tables created in this transaction live on the transaction, not here, so they are safe.
		for (auto it = entries.begin(); it != entries.end();) {
			if (listed.find(it->first) == listed.end()) {
				it = entries.erase(it);
			} else {
				++it;
			}
		}
	}
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
	auto casted_val = val.DefaultTryCastAs(type, nullptr, true);
	if (!casted_val) {
		throw InvalidInputException("Can't cast '%s' property (%s) to %s", property_name, val.ToString(),
		                            type.ToString());
	}
	return std::move(*casted_val);
}

shared_ptr<IcebergTable> IcebergTableSet::CreateEntryInternal(const string &name, IcebergTable &&table,
                                                              shared_ptr<IcebergTable> &old_entry) {
	auto it = entries.find(name);
	if (it != entries.end()) {
		old_entry = std::move(it->second);
		it->second = make_shared_ptr<IcebergTable>(std::move(table));
	} else {
		it = entries.emplace(name, make_shared_ptr<IcebergTable>(std::move(table))).first;
	}
	return it->second;
}

IcebergTable &IcebergTableSet::CreateNewEntry(ClientContext &context, IcebergCatalog &catalog,
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
		Value default_version_value;
		if (context.TryGetCurrentSetting(DEFAULT_FORMAT_VERSION_CONFIG_VARIABLE, default_version_value)) {
			iceberg_version = default_version_value.GetValue<uint64_t>();
		} else {
			iceberg_version = DEFAULT_ICEBERG_FORMAT_VERSION;
		}
	}

	string location;
	auto location_it = info.options.find("location");
	if (location_it != info.options.end()) {
		location = ParseTableProperty(property_binder, context, *location_it->second, "location", LogicalType::VARCHAR)
		               .GetValue<string>();
	}
	if (location.empty() && catalog.attach_options.default_table_location_from_namespace) {
		schema.LoadProperties(context);
		auto ns_location_it = schema.schema_info.properties.find("location");
		if (ns_location_it != schema.schema_info.properties.end() && !ns_location_it->second.empty()) {
			location = ns_location_it->second;
			StringUtil::RTrim(location, "/");
			location += "/" + info.GetTableName().GetIdentifierName();
		}
	}

	IcebergTableMetadata bootstrap_metadata(IcebergTableMetadataSchemas {});
	bootstrap_metadata.iceberg_version = iceberg_version.GetIndex();
	int32_t last_column_id;

	auto new_schema = IcebergCreateTableRequest::CreateIcebergSchema(context, bootstrap_metadata, info.columns,
	                                                                 &info.constraints, last_column_id);
	new_schema->schema_id = 0;
	bootstrap_metadata.last_column_id = last_column_id;
	bootstrap_metadata.SetCurrentSchemaId(0);

	// Get Location
	if (!location.empty()) {
		bootstrap_metadata.location = location;
	}
	for (auto &option : info.options) {
		if (option.first == "format-version" || option.first == "location") {
			continue;
		}
		auto option_val =
		    ParseTableProperty(property_binder, context, *option.second, option.first, LogicalType::VARCHAR)
		        .GetValue<string>();
		bootstrap_metadata.table_properties.emplace(option.first, option_val);
	}

	auto initial_partition_spec = IcebergTable::BuildPartitionSpec(info.partition_keys, *new_schema, 0, 1000);
	//! Sort order id 0 is reserved for the unsorted order, so a table created with SORTED BY starts at 1
	auto initial_sort_order_id = info.sort_keys.empty() ? UNSORTED_SORT_ORDER_ID : INITIAL_SORT_ORDER_ID;
	auto initial_sort_order = IcebergTable::BuildSortOrder(context, info.sort_keys, *new_schema, initial_sort_order_id);
	IcebergCreateTableRequest create_table_request(info.GetTableName().GetIdentifierName(), new_schema,
	                                               std::move(initial_partition_spec), std::move(initial_sort_order),
	                                               iceberg_version.GetIndex(), bootstrap_metadata.table_properties,
	                                               bootstrap_metadata.location);

	// Immediately create the table with stage_create = true to get metadata & data location(s)
	// transaction commit will either commit with data (OR) create the table with stage_create = false
	auto new_table_result = make_uniq<const rest_api_objects::LoadTableResult>(
	    IRCAPI::CommitNewTable(context, catalog, schema.namespace_items, create_table_request));

	auto key = IcebergTable::GetTableKey(catalog, schema.namespace_items, info.GetTableName().GetIdentifierName());
	auto &load_table_result = *new_table_result;
	auto &alter_update = iceberg_transaction.GetOrCreateAlter();
	auto &table_info = alter_update.CreateTable(
	    key, IcebergTable(catalog, schema, info.GetTableName().GetIdentifierName(), load_table_result));
	catalog.table_request_cache.SetOrOverwrite(key, std::move(new_table_result));

	// if we stage created the table, we add an assert create
	auto &transaction_data = table_info.GetOrCreateTransactionData(iceberg_transaction);
	if (catalog.attach_options.stage_create_tables) {
		transaction_data.TableAddAssertCreate();
	}
	if (!catalog.attach_options.stage_create_tables && catalog.attach_options.skip_create_table_metadata_updates) {
		return table_info;
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
	transaction_data.TableSetProperties(table_info.table_metadata.table_properties);

	iceberg_transaction.SetLatestTableState(key, IcebergTableStatus::ALIVE);
	return table_info;
}

static bool EntryMissingFromListing(const optional<vector<rest_api_objects::TableIdentifier>> &listing,
                                    const string &name) {
	// A refused listing is not evidence that an entry is absent: retain the direct lookup.
	if (!listing) {
		return false;
	}
	for (auto &entry : *listing) {
		if (StringUtil::CIEquals(entry.name, name)) {
			return false;
		}
	}
	return true;
}

optional_ptr<CatalogEntry> IcebergTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);
	const auto &table_name = lookup.GetEntryName();
	// first check transaction entries
	const auto table_key = IcebergTable::GetTableKey(ic_catalog, schema.namespace_items, table_name);
	auto latest_state = iceberg_transaction.GetLatestTableState(table_key);

	auto at = lookup.GetAtClause();
	if (latest_state) {
		if (!latest_state->IsAlive()) {
			// If table has been deleted or is missing within the transaction, return null
			return nullptr;
		}
		auto &table_info = latest_state->GetInfo();
		if (table_info.schema_versions.empty()) {
			table_info.InitSchemaVersions();
		}
		return table_info.GetSchemaVersion(at);
	}

	// File replacement scans first probe the catalog. Some REST servers reject encoded
	// path separators before routing, so use the listing to rule out absent path names.
	// Listed entries and transaction-local tables still take precedence over files.
	if (table_name.find_first_of("/\\") != string::npos &&
	    EntryMissingFromListing(IRCAPI::GetTables(context, ic_catalog, schema), table_name)) {
		return nullptr;
	}
	auto new_version = IcebergTable::CreatePlaceholder(ic_catalog, schema, table_name);
	auto &table_info = *new_version;
	if (!FillEntry(context, table_info)) {
		//! The table doesn't exist in the catalog
		iceberg_transaction.SetLatestTableState(table_key, IcebergTableStatus::MISSING);
		return nullptr;
	}

	{
		annotated_lock_guard<annotated_mutex> l(entry_lock);
		entries[table_name] = new_version;
	}
	iceberg_transaction.tables[table_key] = new_version;
	auto &state = iceberg_transaction.SetCatalogTableState(new_version);
	if (iceberg_transaction.StartedBefore(table_info.table_metadata.last_updated_ms)) {
		state.GetOrCreateTransactionInfo(iceberg_transaction);
	}
	return state.GetInfo().GetSchemaVersion(at);
}

// ─── View operations ─────────────────────────────────────────────────────────

const case_insensitive_set_t &IcebergTableSet::LoadViewEntries(ClientContext &context) {
	auto &transaction = IcebergTransaction::Get(context, catalog);
	auto &schema_name = schema.name.GetIdentifierName();
	auto existing = transaction.listed_views.find(schema_name);
	if (existing != transaction.listed_views.end()) {
		return existing->second;
	}
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	IcebergListViewsResult views;
	if (ic_catalog.supported_urls.count("GET /v1/{prefix}/namespaces/{namespace}/views")) {
		IcebergRequestExecutor executor(context, ic_catalog);
		auto result = executor.Schedule(IcebergListViewsRequest(schema.namespace_items));
		views = executor.WaitAndTakeResult(*result);
		context.InterruptCheck();
	}
	return ApplyViewListResult(context, std::move(views));
}

const case_insensitive_set_t &IcebergTableSet::ApplyViewListResult(ClientContext &context,
                                                                   IcebergListViewsResult views) {
	auto &transaction = IcebergTransaction::Get(context, catalog);
	case_insensitive_set_t names;
	if (views) {
		for (auto &view : *views) {
			names.insert(view.name);
		}
	}
	return transaction.listed_views.emplace(schema.name.GetIdentifierName(), std::move(names)).first->second;
}

bool IcebergTableSet::TryGetLocalViewEntry(ClientContext &context, const string &view_name,
                                           optional_ptr<CatalogEntry> &entry) {
	entry = nullptr;
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);
	auto view_key = IcebergTable::GetTableKey(ic_catalog, schema.namespace_items, view_name);
	auto created_it = iceberg_transaction.created_views.find(view_key);
	// A staged replacement is visible after DROP/CREATE in the same transaction.
	if (created_it == iceberg_transaction.created_views.end() && iceberg_transaction.deleted_views.count(view_key)) {
		return true;
	}

	auto cached_it = iceberg_transaction.views.find(view_key);
	if (cached_it != iceberg_transaction.views.end()) {
		entry = cached_it->second.get();
		return true;
	}

	if (created_it != iceberg_transaction.created_views.end()) {
		auto view_entry = make_uniq<ViewCatalogEntry>(catalog, schema, *created_it->second);
		auto result = view_entry.get();
		iceberg_transaction.views.emplace(view_key, std::move(view_entry));
		entry = result;
		return true;
	}

	return false;
}

optional_ptr<CatalogEntry> IcebergTableSet::GetViewEntry(ClientContext &context, const string &view_name) {
	optional_ptr<CatalogEntry> entry;
	if (TryGetLocalViewEntry(context, view_name, entry)) {
		return entry;
	}
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	// Check if the view endpoint is supported
	if (ic_catalog.supported_urls.find("GET /v1/{prefix}/namespaces/{namespace}/views/{view}") ==
	    ic_catalog.supported_urls.end()) {
		return nullptr;
	}

	// Apply the same file-path probe handling as table lookup, after local view resolution.
	if (view_name.find_first_of("/\\") != string::npos &&
	    ic_catalog.supported_urls.count("GET /v1/{prefix}/namespaces/{namespace}/views") &&
	    EntryMissingFromListing(IRCAPI::GetViews(context, ic_catalog, schema), view_name)) {
		return nullptr;
	}

	return ApplyViewLoadResult(context, view_name, IRCAPI::GetView(context, ic_catalog, schema, view_name));
}

optional_ptr<CatalogEntry> IcebergTableSet::ApplyViewLoadResult(ClientContext &context, const string &view_name,
                                                                IcebergLoadViewResult get_view_result) {
	if (get_view_result.error_) {
		if (get_view_result.status_ == HTTPStatusCode::NotFound_404) {
			// View legitimately does not exist in the catalog — let DuckDB report "View ... does not exist".
			return nullptr;
		}
		// 401 / 403 / 500 / etc. — surface the real error rather than silently masking it as "not found".
		throw HTTPException(StringUtil::Format("GetView endpoint returned response code %s with message \"%s\"",
		                                       EnumUtil::ToString(get_view_result.status_),
		                                       get_view_result.error_->_error.message));
	}
	auto &load_result = *get_view_result.result_;

	// Only DuckDB SQL representations are executable.
	string view_sql;
	optional_ptr<const rest_api_objects::ViewVersion> current_version;
	auto &metadata = load_result.metadata;
	// Find the current version
	for (auto &version : metadata.versions) {
		if (version.version_id != metadata.current_version_id) {
			continue;
		}
		current_version = &version;
		// Select the DuckDB representation from the current version.
		for (auto &repr : version.representations) {
			if (!repr.sqlview_representation) {
				continue;
			}
			if (repr.sqlview_representation->dialect == IcebergConstants::ViewDuckDBDialect) {
				view_sql = repr.sqlview_representation->sql;
				break;
			}
		}
		break;
	}

	unique_ptr<SelectStatement> view_query;
	string unsupported_reason;
	if (view_sql.empty()) {
		unsupported_reason = "no SQL representation with dialect 'duckdb'";
	} else if (current_version->default_catalog && Identifier(*current_version->default_catalog) != catalog.GetName()) {
		unsupported_reason = "a different default catalog is not supported";
	} else if (current_version->default_namespace.value != schema.namespace_items) {
		unsupported_reason = "a different default namespace is not supported";
	} else {
		Parser parser;
		try {
			parser.ParseQuery(view_sql);
		} catch (const ParserException &ex) {
			unsupported_reason = "its SQL dialect cannot be parsed by DuckDB";
			DUCKDB_LOG_WARNING(context, "View '%s' SQL could not be parsed by DuckDB: %s", view_name, ex.what());
		}
		if (unsupported_reason.empty()) {
			if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
				unsupported_reason = "its SQL representation must contain a single SELECT statement";
			} else {
				view_query = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
			}
		}
	}

	auto view_info = make_uniq<CreateViewInfo>(schema, Identifier(view_name));
	view_info->query = std::move(view_query);
	view_info->sql = view_sql;
	// The persisted schema owns the output names, including explicit CREATE VIEW aliases.
	if (current_version) {
		for (auto &view_schema : metadata.schemas) {
			if (view_schema.object_1.schema_id && *view_schema.object_1.schema_id == current_version->schema_id) {
				for (auto &field : view_schema.struct_type.fields) {
					view_info->aliases.emplace_back(field->name);
				}
				break;
			}
		}
	}

	unique_ptr<ViewCatalogEntry> view_entry;
	if (unsupported_reason.empty()) {
		view_entry = make_uniq<ViewCatalogEntry>(catalog, schema, *view_info);
	} else {
		view_entry = make_uniq<UnsupportedIcebergViewEntry>(catalog, schema, *view_info, unsupported_reason);
	}
	auto result = view_entry.get();
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);
	auto view_key = IcebergTable::GetTableKey(catalog.Cast<IcebergCatalog>(), schema.namespace_items, view_name);
	iceberg_transaction.views.emplace(view_key, std::move(view_entry));
	return result;
}

void IcebergTableSet::ScanViews(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	// Copy the listing before invoking callbacks: binding a view may resolve other schemas.
	auto names = LoadViewEntries(context);
	auto &transaction = IcebergTransaction::Get(context, catalog);
	for (auto &created : transaction.created_views) {
		auto &info = *created.second;
		if (info.GetQualifiedName().Schema() == schema.name) {
			names.insert(info.GetViewName().GetIdentifierName());
		}
	}
	struct PendingViewLoad {
		string name;
		shared_ptr<IcebergRequestResult<IcebergLoadViewResult>> result;
	};
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto &scheduler = TaskScheduler::GetScheduler(context);
	const auto window_size = MinValue<idx_t>(8, scheduler.NumberOfThreads() + scheduler.NumberOfAsyncThreads());
	deque<PendingViewLoad> pending;
	IcebergRequestExecutor executor(context, ic_catalog);
	auto name = names.begin();
	auto schedule_next = [&]() {
		context.InterruptCheck();
		PendingViewLoad load {*name++, nullptr};
		optional_ptr<CatalogEntry> local;
		// Keep file-path probes on the ordinary lookup path, including their listing checks.
		if (!TryGetLocalViewEntry(context, load.name, local) && load.name.find_first_of("/\\") == string::npos &&
		    ic_catalog.supported_urls.count("GET /v1/{prefix}/namespaces/{namespace}/views/{view}")) {
			load.result = executor.Schedule(IcebergLoadViewRequest(schema.namespace_items, load.name));
		}
		pending.push_back(std::move(load));
	};
	while (name != names.end() && pending.size() < window_size) {
		schedule_next();
	}
	while (!pending.empty()) {
		auto load = std::move(pending.front());
		pending.pop_front();
		optional_ptr<CatalogEntry> entry;
		if (load.result) {
			// Retire the request before refilling, even if a callback made its result obsolete.
			executor.WaitUntilReady(*load.result);
			if (!TryGetLocalViewEntry(context, load.name, entry)) {
				entry = ApplyViewLoadResult(context, load.name, load.result->TakeResult());
			}
		} else {
			entry = GetViewEntry(context, load.name);
		}
		if (name != names.end()) {
			schedule_next();
		}
		if (entry) {
			callback(*entry);
		}
	}
	executor.Drain();
}

} // namespace duckdb
