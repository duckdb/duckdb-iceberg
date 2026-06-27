#include "catalog/rest/transaction/iceberg_transaction.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/storage/table/update_state.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/main/client_data.hpp"
#include "yyjson.hpp"

#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/storage/iceberg_authorization.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/api/iceberg_add_snapshot.hpp"
#include "catalog/rest/api/iceberg_create_table_request.hpp"
#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "iceberg_logging.hpp"
#include "catalog/rest/api/table_update.hpp"
#include "catalog/rest/transaction/iceberg_transaction_update.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {

IcebergTransactionTableState::IcebergTransactionTableState(optional_ptr<IcebergTableInformation> table)
    : table(table), status(table ? IcebergTableStatus::ALIVE : IcebergTableStatus::MISSING) {
}

IcebergTransactionData &IcebergTransactionTableState::GetOrCreateTransactionData(IcebergTransaction &transaction) {
	if (!transaction_data) {
		auto context = transaction.context.lock();
		transaction_data = make_uniq<IcebergTransactionData>(*context, GetInfo(), GetBaseMetadata());
	}
	return *transaction_data;
}

IcebergTableMetadata IcebergTransactionTableState::GetTransactionMetadata() const {
	auto metadata = GetBaseMetadata().Copy();
	if (!transaction_data || !transaction_data->HasUpdates()) {
		return metadata;
	}
	return transaction_data->GetTransactionMetadata(metadata);
}

IcebergTableEntry &IcebergTransactionTableState::GetOrCreateSchemaEntry(const IcebergTableSchema &table_schema) {
	auto it = schema_versions.find(table_schema.schema_id);
	if (it != schema_versions.end()) {
		return *it->second;
	}

	CreateTableInfo info;
	info.table = Identifier(GetInfo().name);
	for (auto &col : table_schema.columns) {
		info.columns.AddColumn(col->GetColumnDefinition());
	}

	auto table_entry = make_uniq<IcebergTableEntry>(GetInfo(), GetInfo().catalog, GetInfo().schema, info,
	                                                table_schema.schema_id, *this);
	if (!table_entry->internal) {
		table_entry->internal = GetInfo().schema.internal;
	}
	auto &result = *table_entry;
	schema_versions.emplace(table_schema.schema_id, std::move(table_entry));
	return result;
}

IcebergTableEntry &IcebergTransactionTableState::GetOrCreateDummyEntry() {
	if (dummy_entry) {
		return *dummy_entry;
	}
	CreateTableInfo info(GetInfo().schema, Identifier(GetInfo().name));
	vector<ColumnDefinition> columns;
	columns.emplace_back(Identifier("__"), LogicalType::UNKNOWN);
	info.columns = ColumnList(std::move(columns));
	dummy_entry =
	    make_uniq<IcebergTableEntry>(GetInfo(), GetInfo().catalog, GetInfo().schema, info, optional_idx(), *this);
	if (!dummy_entry->internal) {
		dummy_entry->internal = GetInfo().schema.internal;
	}
	return *dummy_entry;
}

optional_ptr<CatalogEntry> IcebergTransactionTableState::GetLatestSchema(ClientContext &context) {
	return GetSchemaVersion(context, nullptr);
}

optional_ptr<CatalogEntry> IcebergTransactionTableState::GetSchemaVersion(ClientContext &context,
                                                                          optional_ptr<BoundAtClause> at) {
	auto metadata = GetTransactionMetadata();
	if (metadata.snapshots.empty()) {
		auto schema = metadata.GetSchemaFromId(metadata.GetCurrentSchemaId());
		D_ASSERT(schema);
		return GetOrCreateSchemaEntry(*schema);
	}

	auto &meta_transaction = MetaTransaction::Get(context);
	auto transaction_start = meta_transaction.GetCurrentTransactionStartTimestamp();
	auto transaction_start_millis = Timestamp::GetEpochMs(transaction_start);

	auto snapshot_lookup = IcebergSnapshotLookup::FromAtClause(at);
	auto snapshot_info = metadata.GetSnapshot(snapshot_lookup);

	int32_t schema_id;
	if (!snapshot_lookup.IsLatest() && snapshot_info.snapshot) {
		auto &snapshot = *snapshot_info.snapshot;
		if (snapshot.timestamp_ms.value > transaction_start.value) {
			return nullptr;
		}
		schema_id = snapshot.GetSchemaId();
	} else {
		if (transaction_data && transaction_data->HasUpdates()) {
			schema_id = metadata.GetCurrentSchemaId();
		} else {
			bool use_metadata_log = true;
			Value val;
			if (context.TryGetCurrentSetting("iceberg_use_metadata_log", val)) {
				if (!val.IsNull() && val.type().id() == LogicalTypeId::BOOLEAN) {
					use_metadata_log = val.GetValue<bool>();
				}
			}

			const bool latest_metadata_is_too_fresh = metadata.last_updated_ms.value > transaction_start_millis;
			const bool can_use_metadata_log = use_metadata_log && !metadata.metadata_log.empty();
			if (latest_metadata_is_too_fresh && can_use_metadata_log) {
				string metadata_path;
				auto relevant_metadata =
				    GetInfo().CreateMetadataFromLog(context, transaction_start_millis, metadata_path);
				schema_id = relevant_metadata.GetCurrentSchemaId();
			} else {
				schema_id = metadata.GetCurrentSchemaId();
			}
		}
	}
	auto schema = metadata.GetSchemaFromId(schema_id);
	if (!schema) {
		return nullptr;
	}
	return GetOrCreateSchemaEntry(*schema);
}

IcebergTransaction::IcebergTransaction(IcebergCatalog &ic_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), db(*context.db), catalog(ic_catalog), access_mode(ic_catalog.access_mode),
      transaction_start_timestamp(timestamp_t(0)) {
}

IcebergTransaction::~IcebergTransaction() = default;

void IcebergTransaction::Start() {
	auto ctx = context.lock();
	auto &meta_transaction = MetaTransaction::Get(*ctx);
	transaction_start_timestamp = meta_transaction.GetCurrentTransactionStartTimestamp();
}

IcebergCatalog &IcebergTransaction::GetCatalog() {
	return catalog;
}

static void AddExplicitNullSnapshotIds(yyjson_mut_doc *doc, yyjson_mut_val *root_object,
                                       const rest_api_objects::CommitTableRequest &table) {
	auto requirements_array = yyjson_mut_obj_get(root_object, "requirements");
	D_ASSERT(requirements_array);
	for (idx_t i = 0; i < table.requirements.size(); i++) {
		auto &requirement = table.requirements[i];
		if (!requirement.assert_ref_snapshot_id || requirement.assert_ref_snapshot_id->snapshot_id) {
			continue;
		}
		auto requirement_json = yyjson_mut_arr_get(requirements_array, i);
		D_ASSERT(requirement_json);
		yyjson_mut_obj_add_null(doc, requirement_json, "snapshot-id");
	}
}

static yyjson_mut_val *CommitTableToJSON(yyjson_mut_doc *doc, const rest_api_objects::CommitTableRequest &table) {
	auto root_object = table.ToJSON(doc);
	AddExplicitNullSnapshotIds(doc, root_object, table);
	return root_object;
}

static yyjson_mut_val *CommitTransactionToJSON(yyjson_mut_doc *doc,
                                               const rest_api_objects::CommitTransactionRequest &req) {
	auto root_object = req.ToJSON(doc);
	auto table_changes_array = yyjson_mut_obj_get(root_object, "table-changes");
	D_ASSERT(table_changes_array);
	for (idx_t i = 0; i < req.table_changes.size(); i++) {
		auto table_object = yyjson_mut_arr_get(table_changes_array, i);
		D_ASSERT(table_object);
		AddExplicitNullSnapshotIds(doc, table_object, req.table_changes[i]);
	}
	return root_object;
}

string JsonDocToString(std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc) {
	auto root_object = yyjson_mut_doc_get_root(doc.get());

	//! Write the result to a string
	auto data = yyjson_mut_val_write_opts(root_object, YYJSON_WRITE_ALLOW_INF_AND_NAN, nullptr, nullptr, nullptr);
	if (!data) {
		throw InvalidInputException("Could not create a JSON representation of the table schema, yyjson failed");
	}
	auto res = string(data);
	free(data);
	return res;
}

template <class RESTObject>
static string RESTObjectToJSONString(const RESTObject &object) {
	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
	auto doc = doc_p.get();
	yyjson_mut_doc_set_root(doc, object.ToJSON(doc));
	return JsonDocToString(std::move(doc_p));
}

static string ConstructTableUpdateJSON(rest_api_objects::CommitTableRequest &table_change) {
	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
	auto doc = doc_p.get();
	auto root_object = CommitTableToJSON(doc, table_change);
	yyjson_mut_doc_set_root(doc, root_object);
	return JsonDocToString(std::move(doc_p));
}

static rest_api_objects::TableRequirement CreateAssertRefSnapshotIdRequirement(const IcebergSnapshot &old_snapshot) {
	rest_api_objects::TableRequirement req;
	req.assert_ref_snapshot_id = rest_api_objects::AssertRefSnapshotId();

	auto &res = *req.assert_ref_snapshot_id;
	res.ref = "main";
	res.snapshot_id = old_snapshot.snapshot_id;
	res.type.value = "assert-ref-snapshot-id";
	return req;
}

static rest_api_objects::TableRequirement CreateAssertNoSnapshotRequirement() {
	rest_api_objects::TableRequirement req;
	req.assert_ref_snapshot_id = rest_api_objects::AssertRefSnapshotId();

	auto &res = *req.assert_ref_snapshot_id;
	res.ref = "main";
	res.type.value = "assert-ref-snapshot-id";
	return req;
}

void IcebergTransaction::DropSecrets(ClientContext &context) {
	auto &secret_manager = SecretManager::Get(context);
	for (auto &secret_name : created_secrets) {
		(void)secret_manager.DropSecretByName(context, Identifier(secret_name), OnEntryNotFound::RETURN_NULL);
	}
}

static rest_api_objects::TableUpdate CreateSetSnapshotRefUpdate(int64_t snapshot_id) {
	rest_api_objects::TableUpdate table_update;

	table_update.set_snapshot_ref_update = rest_api_objects::SetSnapshotRefUpdate();
	auto &update = *table_update.set_snapshot_ref_update;
	update.base_update.action = "set-snapshot-ref";

	update.ref_name = "main";
	update.snapshot_reference.type = "branch";
	update.snapshot_reference.snapshot_id = snapshot_id;
	return table_update;
}

static bool NeedsAssertSchemaId(const IcebergTransactionData &transaction_data,
                                const IcebergTableInformation &table_info) {
	(void)table_info;
	return transaction_data.assert_schema_id;
}

namespace {

struct SingleTableStagedCommit {
	rest_api_objects::CommitTableRequest request;
	vector<string> created_metadata_files;
	bool retryable = false;
};

static idx_t GetMaxRetries(const IcebergTransactionData &transaction_data) {
	return NumericCast<idx_t>(transaction_data.GetCommitRetryCount());
}

static idx_t GetMaxRetries(const IcebergTransactionAlterUpdate &alter_update) {
	idx_t max_retries = 0;
	bool saw_table = false;
	for (const auto &entry : alter_update.updated_tables) {
		if (alter_update.committed_tables.count(entry.first)) {
			continue;
		}
		auto transaction_data = alter_update.GetTransactionData(entry.first);
		if (!transaction_data || !transaction_data->HasUpdates()) {
			continue;
		}
		auto table_max_retries = GetMaxRetries(*transaction_data);
		if (!saw_table) {
			max_retries = table_max_retries;
			saw_table = true;
		} else {
			max_retries = MinValue<idx_t>(max_retries, table_max_retries);
		}
	}
	return max_retries;
}

static void CreateTableRequirements(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state,
                                    const IcebergTransactionData &transaction_data,
                                    const optional_ptr<const IcebergSnapshot> &current_snapshot) {
	const bool has_assert_create = transaction_data.has_assert_create;
	for (auto &requirement : transaction_data.requirements) {
		requirement->CreateRequirement(db, context, commit_state);
	}
	if (!has_assert_create && NeedsAssertSchemaId(transaction_data, commit_state.table_info)) {
		AssertCurrentSchemaIdRequirement requirement(transaction_data.initial_schema_id);
		requirement.CreateRequirement(db, context, commit_state);
	}
	if (!has_assert_create && transaction_data.HasUpdates()) {
		auto uuid_requirement = AssertTableUUIDRequirement(transaction_data.initial_table_uuid);
		uuid_requirement.CreateRequirement(db, context, commit_state);
	}
	if (current_snapshot && !transaction_data.alters.empty()) {
		commit_state.table_change.requirements.push_back(CreateAssertRefSnapshotIdRequirement(*current_snapshot));
	} else if (!current_snapshot && !transaction_data.alters.empty() && !has_assert_create) {
		commit_state.table_change.requirements.push_back(CreateAssertNoSnapshotRequirement());
	}
}

static SingleTableStagedCommit StageSingleTableCommit(DatabaseInstance &db, IcebergTransactionTableState &table_state,
                                                      IcebergTransactionData &transaction_data,
                                                      ClientContext &context) {
	SingleTableStagedCommit info;
	auto &table_info = table_state.GetInfo();
	auto commit_table = table_info.Copy(context);
	if (!transaction_data.SupportsAppendRetry()) {
		commit_table.table_metadata = table_state.GetTransactionMetadata();
	}
	IcebergCommitState commit_state(commit_table, context);
	auto &table_change = commit_state.table_change;
	auto &schema = table_info.schema.Cast<IcebergSchemaEntry>();
	table_change.identifier = rest_api_objects::TableIdentifier();
	table_change.identifier->_namespace.value = schema.namespace_items;
	table_change.identifier->name = table_info.name;

	auto &metadata = commit_state.table_info.table_metadata;
	auto current_snapshot = metadata.GetLatestSnapshot();
	info.retryable = transaction_data.SupportsAppendRetry();
	if (!transaction_data.alters.empty()) {
		commit_state.LoadExistingManifests(std::move(transaction_data.existing_manifest_list));
	}
	commit_state.latest_snapshot = current_snapshot;

	for (auto &update : transaction_data.updates) {
		if (update->type == IcebergTableUpdateType::ADD_SNAPSHOT) {
			auto &ic_table_entry = table_state.GetLatestSchema(context)->Cast<IcebergTableEntry>();
			ic_table_entry.PrepareIcebergScanFromEntry(context);
		}
		update->CreateUpdate(db, context, commit_state);
	}

	CreateTableRequirements(db, context, commit_state, transaction_data, current_snapshot);

	if (!transaction_data.alters.empty()) {
		auto &snapshot = *commit_state.latest_snapshot;
		auto set_snapshot_ref_update = CreateSetSnapshotRefUpdate(snapshot.snapshot_id);
		commit_state.table_change.updates.push_back(std::move(set_snapshot_ref_update));
	}

	info.created_metadata_files = std::move(commit_state.created_metadata_files);
	info.request = std::move(table_change);
	return info;
}

} // namespace

TableTransactionInfo IcebergTransaction::GetTransactionRequest(IcebergTransactionAlterUpdate &alter_update,
                                                               ClientContext &context) {
	TableTransactionInfo info;
	auto &transaction = info.request;
	bool all_retryable = true;
	bool saw_table = false;
	for (auto &updated_table : alter_update.updated_tables) {
		auto &table_key = updated_table.first;
		if (alter_update.committed_tables.count(table_key)) {
			//! Already committed
			continue;
		}
		auto &table_state = updated_table.second;
		auto transaction_data = alter_update.GetTransactionData(table_key);
		if (!transaction_data || !transaction_data->HasUpdates()) {
			//! No changes to commit
			continue;
		}

		auto table_transaction_info = StageSingleTableCommit(db, table_state, *transaction_data, context);
		info.created_metadata_files.emplace(table_key, std::move(table_transaction_info.created_metadata_files));
		info.table_requests.emplace(table_key, transaction.table_changes.size());
		transaction.table_changes.push_back(std::move(table_transaction_info.request));
		saw_table = true;
		all_retryable = all_retryable && table_transaction_info.retryable;
	}
	info.retryable = saw_table && all_retryable;
	return info;
}

bool IcebergTransaction::CanUseMultiTableCommit(const IcebergTransactionAlterUpdate &alter_update) const {
	if (catalog.attach_options.disable_multi_table_commit ||
	    !catalog.supported_urls.count("POST /v1/{prefix}/transactions/commit")) {
		return false;
	}
	for (const auto &entry : alter_update.updated_tables) {
		auto transaction_data = alter_update.GetTransactionData(entry.first);
		if (!transaction_data) {
			continue;
		}
		if (transaction_data->has_assert_create) {
			return false;
		}
	}
	return true;
}

void IcebergTransaction::CleanupMetadataFiles(ClientContext &context, const vector<string> &paths) {
	if (!catalog.attach_options.remove_files_on_delete || paths.empty()) {
		return;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	unordered_set<string> deleted;
	for (const auto &path : paths) {
		if (!deleted.insert(path).second) {
			continue;
		}
		if (fs.TryRemoveFile(path)) {
			DUCKDB_LOG(context, IcebergLogType, "Iceberg Transaction Cleanup, deleted retry metadata file: '%s'", path);
		} else {
			DUCKDB_LOG(context, IcebergLogType,
			           "Iceberg Transaction Cleanup, failed to delete retry metadata file: '%s'", path);
		}
	}
}

void IcebergTransaction::RefreshRetryTables(IcebergTransactionAlterUpdate &alter_update,
                                            const case_insensitive_set_t &table_keys, ClientContext &context) {
	for (const auto &table_key : table_keys) {
		auto it = alter_update.updated_tables.find(table_key);
		if (it == alter_update.updated_tables.end()) {
			continue;
		}
		auto &table_state = it->second;
		auto transaction_data = alter_update.GetTransactionData(table_key);
		if (!transaction_data) {
			continue;
		}
		auto &base_table = table_state.GetInfo();
		if (!transaction_data->RetryStateMatches(base_table)) {
			throw TransactionException("Table %s changed incompatibly while retrying commit", table_key);
		}
		base_table.RefreshFromCatalog(context);
		if (!transaction_data->RetryStateMatches(base_table)) {
			throw TransactionException("Table %s changed incompatibly while retrying commit", table_key);
		}
		SetLatestTableState(base_table, IcebergTableStatus::ALIVE);
	}
}

static bool CommitIsRetryable(bool retryable, idx_t max_retries, const CommitResult &result, idx_t attempt) {
	if (attempt >= max_retries) {
		//! We've reached the max amount of retries
		return false;
	}
	if (!retryable) {
		//! The operation isn't retryable in general
		return false;
	}
	if (!result.IsConflict()) {
		//! Only conflicts (409) are retryable
		return false;
	}
	return true;
}

static vector<string> GetCreatedMetadataFiles(const TableTransactionInfo &transaction_info) {
	vector<string> created_metadata_files;
	for (const auto &entry : transaction_info.created_metadata_files) {
		created_metadata_files.insert(created_metadata_files.end(), entry.second.begin(), entry.second.end());
	}
	return created_metadata_files;
}

static case_insensitive_set_t GetRetryTableKeys(const TableTransactionInfo &transaction_info) {
	case_insensitive_set_t table_keys;
	for (const auto &entry : transaction_info.table_requests) {
		table_keys.insert(entry.first);
	}
	return table_keys;
}

void IcebergTransaction::Commit() {
	if (transaction_updates.empty() && created_schemas.empty() && deleted_schemas.empty() &&
	    schema_property_updates.empty()) {
		return;
	}

	Connection temp_con(db);
	temp_con.BeginTransaction();
	auto &temp_con_context = temp_con.context;

	// Copy user settings from the original context so that e.g. s3_access_key_id are available
	if (!this->context.expired()) {
		temp_con_context->config = this->context.lock()->config;
	}

	try {
		DoSchemaCreates(*temp_con_context);
		DoSchemaPropertyUpdates(*temp_con_context);
		for (auto &transaction_update : transaction_updates) {
			auto &type = transaction_update->type;
			switch (type) {
			case IcebergTransactionUpdateType::ALTER: {
				auto &alter_update = transaction_update->Cast<IcebergTransactionAlterUpdate>();
				DoTableUpdates(alter_update, *temp_con_context);
				break;
			}
			case IcebergTransactionUpdateType::DELETE: {
				auto &delete_update = transaction_update->Cast<IcebergTransactionDeleteUpdate>();
				DoTableDeletes(delete_update, *temp_con_context);
				break;
			}
			case IcebergTransactionUpdateType::RENAME: {
				auto &rename_update = transaction_update->Cast<IcebergTransactionRenameUpdate>();
				DoTableRename(rename_update, *temp_con_context);
				break;
			}
			default:
				throw InternalException("IcebergTransactionUpdateType (%d) not implemented",
				                        static_cast<uint8_t>(type));
			};
		}
		DoSchemaDeletes(*temp_con_context);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		CleanupFiles();
		DropSecrets(*temp_con_context);
		temp_con.Rollback();
		error.Throw("Failed to commit Iceberg transaction: ");
	}

	temp_con.Rollback();
}

void IcebergTransaction::DoTableUpdates(IcebergTransactionAlterUpdate &alter_update, ClientContext &context) {
	if (!alter_update.HasUpdates()) {
		return;
	}
	if (CanUseMultiTableCommit(alter_update)) {
		DoMultiTableCommitUpdates(alter_update, context);
	} else {
		DoSingleTableCommitUpdates(alter_update, context);
	}

	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	if (ic_catalog.attach_options.max_table_staleness_micros.IsValid()) {
		for (auto &it : alter_update.committed_tables) {
			ic_catalog.table_request_cache.Expire(context, it);
		}
	}
	DropSecrets(context);
}

void IcebergTransaction::DoTableRename(IcebergTransactionRenameUpdate &rename_update, ClientContext &context) {
	auto &original_table = rename_update.table;
	auto &schema = original_table.schema;
	auto table_key = original_table.GetTableKey();
	auto &table_name = original_table.name;
	auto new_name = rename_update.new_name;

	rest_api_objects::RenameTableRequest request;
	request.source._namespace.value = schema.namespace_items;
	request.source.name = table_name;
	request.destination._namespace.value = schema.namespace_items;
	request.destination.name = new_name;
	auto transaction_json = RESTObjectToJSONString(request);
	IRCAPI::CommitTableRename(context, catalog, transaction_json);

	DropInfo drop_info;
	drop_info.name = Identifier(table_name);
	drop_info.if_not_found = OnEntryNotFound::THROW_EXCEPTION;
	schema.DropEntry(context, drop_info, true);

	lock_guard<mutex> guard(schema.tables.GetEntryLock());
	shared_ptr<IcebergTableInformation> old_version;
	schema.tables.CreateEntryInternal(guard, new_name, std::move(rename_update.new_table), old_version);
	if (old_version) {
		throw TransactionException("Table %s was already created by a different transaction!", new_name);
	}
}

void IcebergTransaction::DoMultiTableCommitUpdates(IcebergTransactionAlterUpdate &alter_update,
                                                   ClientContext &context) {
	const auto max_retries = GetMaxRetries(alter_update);
	for (idx_t attempt = 0;; attempt++) {
		auto transaction_info = GetTransactionRequest(alter_update, context);
		if (transaction_info.request.table_changes.empty()) {
			alter_update.updated_tables.clear();
			return;
		}

		std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
		auto doc = doc_p.get();
		auto root_object = CommitTransactionToJSON(doc, transaction_info.request);
		yyjson_mut_doc_set_root(doc, root_object);

		auto transaction_json = JsonDocToString(std::move(doc_p));
		auto result = IRCAPI::CommitMultiTableUpdate(context, catalog, transaction_json);
		if (result.Success()) {
			for (auto &it : alter_update.updated_tables) {
				alter_update.committed_tables.insert(it.first);
			}
			return;
		}
		auto created_metadata_files = GetCreatedMetadataFiles(transaction_info);
		if (!CommitIsRetryable(transaction_info.retryable, max_retries, result, attempt)) {
			result.Throw(catalog.GetBaseUrl().GetURLEncoded());
		}
		CleanupMetadataFiles(context, created_metadata_files);
		auto table_keys = GetRetryTableKeys(transaction_info);
		RefreshRetryTables(alter_update, table_keys, context);
	}
}

void IcebergTransaction::DoSingleTableCommitUpdates(IcebergTransactionAlterUpdate &alter_update,
                                                    ClientContext &context) {
	D_ASSERT(catalog.supported_urls.count("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}"));
	for (auto &entry : alter_update.updated_tables) {
		auto &table_key = entry.first;
		auto transaction_data = alter_update.GetTransactionData(table_key);
		const auto max_retries = transaction_data ? GetMaxRetries(*transaction_data) : idx_t(0);
		for (idx_t attempt = 0;; attempt++) {
			if (alter_update.committed_tables.count(table_key)) {
				break;
			}
			auto &table_state = entry.second;
			transaction_data = alter_update.GetTransactionData(table_key);
			if (!transaction_data || !transaction_data->HasUpdates()) {
				break;
			}

			auto table_transaction_info = StageSingleTableCommit(db, table_state, *transaction_data, context);
			auto &table_change = table_transaction_info.request;
			D_ASSERT(table_change.identifier);
			auto &identifier = *table_change.identifier;
			auto transaction_json = ConstructTableUpdateJSON(table_change);
			auto result = IRCAPI::CommitTableUpdate(context, catalog, identifier._namespace.value, identifier.name,
			                                        transaction_json);
			if (result.Success()) {
				alter_update.committed_tables.insert(table_key);
				break;
			}

			if (!CommitIsRetryable(table_transaction_info.retryable, max_retries, result, attempt)) {
				result.Throw(catalog.GetBaseUrl().GetURLEncoded());
			}
			CleanupMetadataFiles(context, table_transaction_info.created_metadata_files);
			case_insensitive_set_t retry_tables;
			retry_tables.insert(table_key);
			RefreshRetryTables(alter_update, retry_tables, context);
		}
	}
}

void IcebergTransaction::DoTableDeletes(IcebergTransactionDeleteUpdate &delete_update, ClientContext &context) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto &table = delete_update.deleted_table;
	auto schema_key = table.schema.name;
	auto table_key = table.GetTableKey();
	auto &table_name = table.name;
	IRCAPI::CommitTableDelete(context, catalog, table.schema.namespace_items, table_name);
	// remove the load table result
	ic_catalog.table_request_cache.Expire(context, table_key);
	// remove the table entry from the catalog
	auto &schema_entry = ic_catalog.schemas.GetEntry(schema_key.GetIdentifierName()).Cast<IcebergSchemaEntry>();
	DropInfo drop_info;
	drop_info.name = Identifier(table_name);
	drop_info.if_not_found = OnEntryNotFound::RETURN_NULL;
	schema_entry.DropEntry(context, drop_info, true);
}

void IcebergTransaction::DoSchemaCreates(ClientContext &context) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	for (auto &schema_name : created_schemas) {
		auto namespace_identifiers = IRCAPI::ParseSchemaName(schema_name);

		rest_api_objects::CreateNamespaceRequest request;
		request._namespace.value = namespace_identifiers;
		request.properties = case_insensitive_map_t<string>();
		auto create_body = RESTObjectToJSONString(request);

		IRCAPI::CommitNamespaceCreate(context, ic_catalog, create_body);
	}
	created_schemas.clear();
}

void IcebergTransaction::DoSchemaDeletes(ClientContext &context) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	for (auto &schema_name : deleted_schemas) {
		vector<string> namespace_items;
		auto namespace_identifier = IRCAPI::ParseSchemaName(schema_name);
		IRCAPI::CommitNamespaceDrop(context, ic_catalog, namespace_identifier);
		ic_catalog.GetSchemas().RemoveEntry(schema_name);
	}
	deleted_schemas.clear();
}

void IcebergTransaction::DoSchemaPropertyUpdates(ClientContext &context) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	for (auto &properties_update : this->schema_property_updates) {
		auto schema_name_with_catalog = properties_update.first;
		auto catalog_splitter = schema_name_with_catalog.find(".");
		auto schema_name_no_catalog = schema_name_with_catalog.erase(0, catalog_splitter + 1);

		auto schema_property_updates = properties_update.second;
		auto namespace_identifiers = IRCAPI::ParseSchemaName(schema_name_no_catalog);

		rest_api_objects::UpdateNamespacePropertiesRequest request;
		request.removals = vector<string>();
		request.removals->assign(schema_property_updates.removals.begin(), schema_property_updates.removals.end());
		request.updates = schema_property_updates.updates;
		auto create_body = RESTObjectToJSONString(request);

		IRCAPI::CommitNamespacePropertiesUpdate(context, ic_catalog, create_body, namespace_identifiers);
	}
	created_schemas.clear();
}

namespace {

struct ScopedTransaction {
public:
	ScopedTransaction(DatabaseInstance &db) : connection(db) {
		connection.BeginTransaction();
	}
	~ScopedTransaction() {
		//! Prevent the connection from destructing with an active transaction
		//! As that causes it to ROLLBACK and enter CleanupFiles - resulting in a stack overflow due to recursion
		connection.Commit();
	}

public:
	ClientContext &GetContext() {
		return *connection.context;
	}

public:
	Connection connection;
};

} // namespace

void IcebergTransaction::CleanupFiles() {
	// remove any files that were written
	if (!catalog.attach_options.remove_files_on_delete) {
		// certain catalogs don't allow deletes and will have a s3.deletes attribute in the config describing this
		// aws s3 tables rejects deletes and will handle garbage collection on its own, any attempt to delete the files
		// on the aws side will result in an error.
		return;
	}
	ScopedTransaction temp_con(db);
	auto &temp_context = temp_con.GetContext();
	auto &fs = FileSystem::GetFileSystem(temp_context);

	for (auto &transaction_update : transaction_updates) {
		if (transaction_update->type != IcebergTransactionUpdateType::ALTER) {
			continue;
		}
		auto &alter_update = transaction_update->Cast<IcebergTransactionAlterUpdate>();
		for (auto &up_table : alter_update.updated_tables) {
			if (alter_update.committed_tables.count(up_table.first)) {
				//! Successfully committed, no need to roll back
				continue;
			}
			auto &table = up_table.second;
			auto transaction_data = alter_update.GetTransactionData(up_table.first);
			if (!transaction_data) {
				// error occurred before transaction data was initialized
				// this can happen during table creation with table schema that cannot convert to
				// an iceberg table schema due to type incompatabilities
				continue;
			}
			for (auto &update : transaction_data->updates) {
				if (update->type != IcebergTableUpdateType::ADD_SNAPSHOT) {
					continue;
				}
				// we need to recreate the keys in the current context.
				auto &ic_table_entry = table.GetLatestSchema(temp_context)->Cast<IcebergTableEntry>();
				ic_table_entry.PrepareIcebergScanFromEntry(temp_context);

				auto &add_snapshot = update->Cast<IcebergAddSnapshot>();
				const auto manifest_list_entries = add_snapshot.GetManifestFiles();
				for (const auto &manifest : manifest_list_entries) {
					for (auto &manifest_entry : manifest.manifest_entries) {
						auto &data_file = manifest_entry.data_file;
						if (fs.TryRemoveFile(data_file.file_path)) {
							DUCKDB_LOG(temp_context, IcebergLogType,
							           "Iceberg Transaction Cleanup, deleted 'data_file': '%s'", data_file.file_path);
						}
					}
				}
			}
		}
	}
}

void IcebergTransaction::Rollback() {
	CleanupFiles();
}

IcebergTransaction &IcebergTransaction::Get(ClientContext &context, Catalog &catalog) {
	D_ASSERT(catalog.GetCatalogType() == "iceberg");
	return Transaction::Get(context, catalog).Cast<IcebergTransaction>();
}

bool IcebergTransaction::StartedBefore(timestamp_t timestamp_ms) const {
	auto start = Timestamp::GetEpochMs(transaction_start_timestamp);
	return start < timestamp_ms.value;
}

optional_ptr<IcebergTransactionTableState> IcebergTransaction::GetLatestTableState(const string &table_key) {
	auto current_it = current_table_data.find(table_key);
	if (current_it != current_table_data.end() && !current_it->second.IsAlive()) {
		return current_it->second;
	}
	for (idx_t i = transaction_updates.size(); i-- > 0;) {
		auto &transaction_update = transaction_updates[i];
		if (transaction_update->type != IcebergTransactionUpdateType::ALTER) {
			continue;
		}
		auto &alter_update = transaction_update->Cast<IcebergTransactionAlterUpdate>();
		auto alter_it = alter_update.updated_tables.find(table_key);
		if (alter_it != alter_update.updated_tables.end()) {
			return alter_it->second;
		}
	}
	if (current_it == current_table_data.end()) {
		return nullptr;
	}
	return current_it->second;
}

IcebergTransactionTableState &IcebergTransaction::SetLatestTableState(const string &table_key,
                                                                      IcebergTableStatus status) {
	auto it = current_table_data.find(table_key);
	if (it == current_table_data.end()) {
		it = current_table_data.emplace(table_key, IcebergTransactionTableState(nullptr)).first;
	}
	it->second.SetStatus(status);
	return it->second;
}

IcebergTransactionTableState &IcebergTransaction::SetLatestTableState(IcebergTableInformation &table,
                                                                      IcebergTableStatus status) {
	auto table_key = table.GetTableKey();
	auto &state = SetLatestTableState(table_key, status);
	state.SetTable(table);
	return state;
}

IcebergTransactionAlterUpdate &IcebergTransaction::GetOrCreateAlter() {
	if (transaction_updates.empty() || transaction_updates.back()->type != IcebergTransactionUpdateType::ALTER) {
		auto alter_p = make_uniq<IcebergTransactionAlterUpdate>(*this);
		auto &alter = *alter_p;
		transaction_updates.push_back(std::move(alter_p));
		return alter;
	}
	return transaction_updates.back()->Cast<IcebergTransactionAlterUpdate>();
}

optional_ptr<IcebergTransactionData> IcebergTransaction::GetTransactionData(const string &table_key) const {
	for (idx_t i = transaction_updates.size(); i-- > 0;) {
		auto &transaction_update = transaction_updates[i];
		if (transaction_update->type != IcebergTransactionUpdateType::ALTER) {
			continue;
		}
		auto &alter_update = transaction_update->Cast<IcebergTransactionAlterUpdate>();
		auto transaction_data = alter_update.GetTransactionData(table_key);
		if (transaction_data) {
			return transaction_data;
		}
	}
	return nullptr;
}

bool IcebergTransaction::HasTransactionUpdates(const string &table_key) const {
	auto transaction_data = GetTransactionData(table_key);
	return transaction_data && transaction_data->HasUpdates();
}

IcebergTableInformation &IcebergTransaction::DeleteTable(IcebergTableInformation &table) {
	auto table_key = table.GetTableKey();
	auto state = GetLatestTableState(table_key);

	unique_ptr<IcebergTransactionDeleteUpdate> delete_update;
	if (state) {
		auto &table_info = state->GetInfo();
		delete_update = make_uniq<IcebergTransactionDeleteUpdate>(*this, table_info);
	} else {
		delete_update = make_uniq<IcebergTransactionDeleteUpdate>(*this, table);
	}
	auto &deleted_table = delete_update->deleted_table;
	state = SetLatestTableState(deleted_table, IcebergTableStatus::DROPPED);
	transaction_updates.push_back(std::move(delete_update));
	return state->GetInfo();
}

IcebergTableInformation &IcebergTransaction::RenameTable(IcebergTableInformation &table, const string &new_name) {
	auto table_key = table.GetTableKey();
	auto state = GetLatestTableState(table_key);
	if (state) {
		auto &original_table = state->GetInfo();
		if (HasTransactionUpdates(original_table.GetTableKey())) {
			throw CatalogException("This table (%s) was modified already, can't be renamed!", table.name);
		}
	}

	state = SetLatestTableState(table, IcebergTableStatus::RENAMED);

	//! Create the rename update, creating the new IcebergTableInformation in the process
	auto rename = make_uniq<IcebergTransactionRenameUpdate>(*this, state->GetInfo(), new_name);
	auto &rename_update = *rename;
	transaction_updates.push_back(std::move(rename));

	//! Update the state of the renamed table
	auto &new_table = rename_update.new_table;
	SetLatestTableState(new_table, IcebergTableStatus::ALIVE);
	new_table.InitSchemaVersions();

	auto locked_context = context.lock();
	auto &client_context = *locked_context;
	//! FIXME: just like the other place, this can easily go wrong
	//! Migrate the MetadataCache
	auto new_table_key = new_table.GetTableKey();
	auto &table_request_cache = catalog.table_request_cache;
	lock_guard<mutex> cache_guard(table_request_cache.Lock());
	auto cache = table_request_cache.Get(client_context, table_key, cache_guard, false);
	table_request_cache.SetOrOverwriteInternal(cache_guard, client_context, new_table_key, cache->expire_timestamp,
	                                           std::move(cache->load_table_result));
	table_request_cache.ExpireInternal(cache_guard, client_context, table_key);
	return state->GetInfo();
}

void ApplyTableUpdate(IcebergTableInformation &table_info, IcebergTransaction &iceberg_transaction,
                      const std::function<void(IcebergTransactionTableState &, IcebergTransactionData &)> &callback) {
	auto &alter = iceberg_transaction.GetOrCreateAlter();
	auto &updated_table = alter.GetOrInitializeTable(table_info);
	auto &transaction_data = alter.GetOrCreateTransactionData(updated_table);
	callback(updated_table, transaction_data);
}

} // namespace duckdb
