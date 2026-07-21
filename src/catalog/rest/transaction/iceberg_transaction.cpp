#include "catalog/rest/transaction/iceberg_transaction.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/storage/table/update_state.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/main/client_data.hpp"
#include "yyjson.hpp"

#include <chrono>
#include <optional>
#include <random>
#include <thread>

#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/api/iceberg_retry.hpp"
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
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "iceberg_logging.hpp"
#include "catalog/rest/api/table_update.hpp"
#include "catalog/rest/transaction/iceberg_transaction_update.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {

IcebergTransactionTableState::IcebergTransactionTableState() : status(IcebergTableStatus::MISSING) {
}

IcebergTransactionTableState::IcebergTransactionTableState(shared_ptr<IcebergTableInformation> catalog_table)
    : catalog_table(std::move(catalog_table)), status(IcebergTableStatus::ALIVE) {
}

IcebergTransactionTableState::IcebergTransactionTableState(IcebergTableInformation &&transaction_table_p)
    : transaction_table(make_uniq<IcebergTableInformation>(std::move(transaction_table_p))),
      status(IcebergTableStatus::ALIVE) {
	if (!transaction_table->table_metadata.GetSchemas().empty()) {
		transaction_table->InitSchemaVersions();
	}
}

const IcebergTableInformation &IcebergTransactionTableState::GetInfo() const {
	return const_cast<IcebergTransactionTableState &>(*this).GetInfo();
}

IcebergTableInformation &IcebergTransactionTableState::GetOrCreateTransactionInfo(IcebergTransaction &transaction) {
	if (transaction_table) {
		return *transaction_table;
	}
	if (!catalog_table) {
		throw InternalException("Cannot materialize transaction table state without table information");
	}
	transaction_table = make_uniq<IcebergTableInformation>(catalog_table->Copy(transaction));
	transaction_table->InitSchemaVersions();
	return *transaction_table;
}

IcebergTransaction::IcebergTransaction(IcebergCatalog &ic_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), db(*context.db), catalog(ic_catalog), access_mode(ic_catalog.access_mode) {
}

IcebergTransaction::~IcebergTransaction() = default;

void IcebergTransaction::Start() {
}

IcebergCatalog &IcebergTransaction::GetCatalog() {
	return catalog;
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
	auto root_object = table_change.ToJSON(doc);
	yyjson_mut_doc_set_root(doc, root_object);
	return JsonDocToString(std::move(doc_p));
}

static rest_api_objects::TableRequirement CreateAssertRefSnapshotIdRequirement(const IcebergSnapshot &old_snapshot) {
	rest_api_objects::TableRequirement req;
	req.assert_ref_snapshot_id = rest_api_objects::AssertRefSnapshotId();

	auto &res = *req.assert_ref_snapshot_id;
	res.ref = "main";
	res.snapshot_id = old_snapshot.snapshot_id;
	res.type = "assert-ref-snapshot-id";
	return req;
}

static rest_api_objects::TableRequirement CreateAssertNoSnapshotRequirement() {
	rest_api_objects::TableRequirement req;
	req.assert_ref_snapshot_id = rest_api_objects::AssertRefSnapshotId();

	auto &res = *req.assert_ref_snapshot_id;
	res.ref = "main";
	res.type = "assert-ref-snapshot-id";
	return req;
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
	IcebergRetryConfig retry_config;
};

struct MultiTableStagedCommit {
	rest_api_objects::CommitTransactionRequest request;
	vector<string> created_metadata_files;
	case_insensitive_set_t table_keys;
	bool retryable = false;
	IcebergRetryConfig retry_config;
};

//! Per-retry-loop backoff state: decorrelated jitter (de-synchronizes a thundering herd of
//! concurrent writers) plus a cumulative total-timeout budget. Independent RNG per loop so
//! concurrent committers do not wake in lockstep.
struct IcebergRetryBackoff {
	explicit IcebergRetryBackoff(const IcebergRetryConfig &config_p)
	    : config(config_p), prev_sleep_ms(config_p.min_wait_ms), start(std::chrono::steady_clock::now()),
	      rng(std::random_device {}() ^
	          static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count())),
	      dist(0.0, 1.0) {
	}

	//! Sleep before the next attempt (attempt is the just-failed 0-based attempt index). Returns
	//! false if the wait would exceed commit.retry.total-timeout-ms (only after >=1 retry, mirroring
	//! Java's Tasks.runTaskWithRetry), signalling the caller to stop retrying.
	bool WaitBeforeRetry(idx_t attempt) {
		auto wait_ms = config.DecorrelatedBackoffMs(prev_sleep_ms, dist(rng));
		prev_sleep_ms = wait_ms;
		auto elapsed_ms =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
		if (attempt > 0 && (wait_ms > config.total_wait_ms || elapsed_ms > config.total_wait_ms - wait_ms)) {
			return false;
		}
		if (wait_ms > 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
		}
		return true;
	}

	IcebergRetryConfig config;
	int64_t prev_sleep_ms;
	std::chrono::steady_clock::time_point start;
	std::mt19937_64 rng;
	std::uniform_real_distribution<double> dist;
};

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
	if (!has_assert_create && commit_state.table_info.HasTransactionUpdates()) {
		auto uuid_requirement = AssertTableUUIDRequirement(transaction_data.initial_table_uuid);
		uuid_requirement.CreateRequirement(db, context, commit_state);
	}
	if (current_snapshot && !transaction_data.alters.empty()) {
		commit_state.table_change.requirements.push_back(CreateAssertRefSnapshotIdRequirement(*current_snapshot));
	} else if (!current_snapshot && !transaction_data.alters.empty() && !has_assert_create) {
		commit_state.table_change.requirements.push_back(CreateAssertNoSnapshotRequirement());
	}
}

//! Safe to re-apply a DELETE on retry only if every commit in (base, tip] was a
//! pure append; anything else may have removed/rewritten the target data. Ranges
//! over sequence numbers (parent links aren't populated on read snapshots). A
//! scan/tip snapshot the catalog doesn't expose (e.g. no history) can't be
//! proven safe, so it counts as not reappliable.
static bool DeleteCanReapply(const IcebergTableMetadata &metadata, int64_t base_snapshot_id, int64_t tip_snapshot_id) {
	if (base_snapshot_id == tip_snapshot_id) {
		return true;
	}
	auto base = metadata.FindSnapshotByIdInternal(base_snapshot_id);
	auto tip = metadata.FindSnapshotByIdInternal(tip_snapshot_id);
	if (!base || !tip) {
		return false;
	}
	if (!base->sequence_number || !tip->sequence_number) {
		throw InvalidConfigurationException("Committed snapshot is missing a sequence number");
	}
	int64_t base_sequence = *base->sequence_number;
	int64_t tip_sequence = *tip->sequence_number;
	for (auto &entry : metadata.snapshots) {
		auto &snapshot = entry.second;
		if (!snapshot.sequence_number) {
			throw InvalidConfigurationException("Committed snapshot is missing a sequence number");
		}
		int64_t sequence = *snapshot.sequence_number;
		if (sequence <= base_sequence || sequence > tip_sequence) {
			continue;
		}
		if (snapshot.operation != IcebergSnapshotOperationType::APPEND) {
			return false;
		}
	}
	return true;
}

//! Throw if a retried DELETE can't be safely re-applied. No-op on the first
//! attempt (tip == scan) and for non-delete transactions.
static void VerifyDeleteRetryability(const IcebergTableInformation &table_info,
                                     optional_ptr<const IcebergSnapshot> current_snapshot) {
	if (!table_info.transaction_data) {
		return;
	}
	auto &transaction_data = *table_info.transaction_data;
	if (!transaction_data.ContainsDelete()) {
		return;
	}
	//! No base snapshot: the delete targets data created in this transaction, so there is no
	//! committed base to rebase against and nothing to verify.
	if (!transaction_data.base_snapshot_id) {
		return;
	}
	if (!current_snapshot) {
		return;
	}
	if (!current_snapshot->snapshot_id) {
		throw InvalidConfigurationException("Committed snapshot is missing a snapshot_id");
	}
	auto scan_snapshot_id = *transaction_data.base_snapshot_id;
	auto tip_snapshot_id = *current_snapshot->snapshot_id;
	if (DeleteCanReapply(table_info.table_metadata, scan_snapshot_id, tip_snapshot_id)) {
		return;
	}
	throw TransactionException(
	    "DELETE on \"%s\" conflicts with a concurrent commit that removed or rewrote data (scanned snapshot "
	    "%s, now at %s); re-run the DELETE.",
	    table_info.name, std::to_string(scan_snapshot_id), std::to_string(tip_snapshot_id));
}

static SingleTableStagedCommit StageSingleTableCommit(DatabaseInstance &db, IcebergTableInformation &table_info,
                                                      ClientContext &context) {
	SingleTableStagedCommit info;
	IcebergCommitState commit_state(table_info, context);
	auto &table_change = commit_state.table_change;
	auto &schema = table_info.schema.Cast<IcebergSchemaEntry>();
	table_change.identifier = rest_api_objects::TableIdentifier();
	table_change.identifier->_namespace.value = schema.namespace_items;
	table_change.identifier->name = table_info.name;

	auto &metadata = commit_state.table_info.table_metadata;
	auto current_snapshot = metadata.GetLatestSnapshot();
	auto &transaction_data = *commit_state.table_info.transaction_data;
	info.retryable = transaction_data.SupportsAppendRetry();
	info.retry_config = IcebergRetryConfig::FromTableMetadata(metadata);
	if (!transaction_data.alters.empty()) {
		table_info.LoadCredentials(context);
		commit_state.LoadExistingManifests(db, std::move(transaction_data.existing_manifest_list));
	}
	commit_state.latest_snapshot = current_snapshot;

	VerifyDeleteRetryability(commit_state.table_info, current_snapshot);

	for (auto &update : transaction_data.updates) {
		update->CreateUpdate(db, context, commit_state);
	}

	CreateTableRequirements(db, context, commit_state, transaction_data, current_snapshot);

	if (!transaction_data.alters.empty()) {
		auto &snapshot = *commit_state.latest_snapshot;
		if (!snapshot.snapshot_id) {
			throw InvalidConfigurationException("snapshot.snapshot_id is not set");
		}
		auto set_snapshot_ref_update = CreateSetSnapshotRefUpdate(*snapshot.snapshot_id);
		commit_state.table_change.updates.push_back(std::move(set_snapshot_ref_update));
	}

	if (transaction_data.pending_current_schema_id.has_value()) {
		SetCurrentSchema update(*transaction_data.pending_current_schema_id);
		update.CreateUpdate(db, context, commit_state);
	}

	info.created_metadata_files = std::move(commit_state.created_metadata_files);
	info.request = std::move(table_change);
	return info;
}

static MultiTableStagedCommit StageMultiTableCommit(DatabaseInstance &db, IcebergTransactionAlterUpdate &alter_update,
                                                    ClientContext &context) {
	MultiTableStagedCommit info;
	auto &transaction = info.request;
	bool all_retryable = true;
	bool saw_table = false;
	for (auto &updated_table : alter_update.updated_tables) {
		auto &table_info = updated_table.second.get();
		if (!table_info.HasTransactionUpdates()) {
			//! No changes to commit
			continue;
		}

		auto table_transaction_info = StageSingleTableCommit(db, table_info, context);
		info.created_metadata_files.insert(info.created_metadata_files.end(),
		                                   table_transaction_info.created_metadata_files.begin(),
		                                   table_transaction_info.created_metadata_files.end());
		info.table_keys.insert(updated_table.first);
		transaction.table_changes.push_back(std::move(table_transaction_info.request));
		//! Tables in one atomic transaction share a single retry loop, so fold their retry policies
		//! into the most lenient one (first table seeds it, the rest are merged in).
		info.retry_config = saw_table ? info.retry_config.MostLenient(table_transaction_info.retry_config)
		                              : table_transaction_info.retry_config;
		saw_table = true;
		all_retryable = all_retryable && table_transaction_info.retryable;
	}
	info.retryable = saw_table && all_retryable;
	return info;
}

static optional_ptr<IcebergTableInformation> GetSingleUpdatedTable(IcebergTransactionAlterUpdate &alter_update) {
	optional_ptr<IcebergTableInformation> result;
	for (auto &entry : alter_update.updated_tables) {
		auto &table_info = entry.second.get();
		if (!table_info.HasTransactionUpdates()) {
			continue;
		}
		if (result) {
			throw InternalException("Single-table Iceberg commit staged multiple tables");
		}
		result = table_info;
	}
	return result;
}

} // namespace

bool IcebergTransaction::CanUseMultiTableCommit(const IcebergTransactionAlterUpdate &alter_update) const {
	if (!MultiTableCommitAvailable()) {
		return false;
	}
	for (const auto &entry : alter_update.updated_tables) {
		const auto &table_info = entry.second.get();
		if (!table_info.transaction_data) {
			continue;
		}
		if (table_info.transaction_data->has_assert_create) {
			return false;
		}
	}
	return true;
}

void IcebergTransaction::VerifyAlterUpdateAtomicity(const IcebergTransactionAlterUpdate &alter_update) const {
	if (alter_update.updated_tables.size() <= 1) {
		return;
	}
	if (CanUseMultiTableCommit(alter_update)) {
		return;
	}
	throw TransactionException("Iceberg REST Catalog cannot commit this transaction atomically because it would "
	                           "require multiple table commit requests without atomic multi-table commit support");
}

bool IcebergTransaction::MultiTableCommitAvailable() const {
	return !catalog.attach_options.disable_multi_table_commit &&
	       catalog.supported_urls.count("POST /v1/{prefix}/transactions/commit");
}

bool IcebergTransaction::HasTableUpdate() const {
	return !std::holds_alternative<std::monostate>(transaction_update);
}

IcebergTransactionAlterUpdate *IcebergTransaction::GetAlterUpdate() {
	return std::get_if<IcebergTransactionAlterUpdate>(&transaction_update);
}

const IcebergTransactionAlterUpdate *IcebergTransaction::GetAlterUpdate() const {
	return std::get_if<IcebergTransactionAlterUpdate>(&transaction_update);
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
		auto &table_info = it->second.get();
		if (!table_info.transaction_data) {
			continue;
		}
		if (!table_info.transaction_data->RetryStateMatches(table_info)) {
			throw TransactionException("Table %s changed incompatibly while retrying commit", table_key);
		}
		table_info.RefreshFromCatalog(context);
		if (!table_info.transaction_data->RetryStateMatches(table_info)) {
			throw TransactionException("Table %s changed incompatibly while retrying commit", table_key);
		}
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

// 4xx = definitive rejection (commit did not land) -> delete the orphan. 5xx / no HTTP status
// (connection error/timeout) = outcome unknown (may have landed) -> keep files. Status is in
// extra_info (HTTPException) or the message "status code (<status>)" (fallback).
static bool CommitStateUnknown(const ErrorData &error) {
	auto &extra = error.ExtraInfo();
	auto it = extra.find("status_code");
	if (it != extra.end()) {
		return it->second.empty() || it->second[0] != '4';
	}
	auto &msg = error.RawMessage();
	auto pos = msg.find("status code (");
	if (pos != string::npos) {
		auto digit = msg.find_first_of("0123456789", pos);
		if (digit != string::npos) {
			return msg[digit] != '4';
		}
	}
	return true;
}

void IcebergTransaction::Commit() {
	if (!HasTableUpdate() && created_schemas.empty() && deleted_schemas.empty() && schema_property_updates.empty()) {
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
		std::visit(
		    [&](auto &update) {
			    using T = std::decay_t<decltype(update)>;
			    if constexpr (std::is_same_v<T, std::monostate>) {
				    return;
			    } else if constexpr (std::is_same_v<T, IcebergTransactionAlterUpdate>) {
				    DoTableUpdates(update, *temp_con_context);
			    } else if constexpr (std::is_same_v<T, IcebergTransactionDeleteUpdate>) {
				    DoTableDeletes(update, *temp_con_context);
			    } else if constexpr (std::is_same_v<T, IcebergTransactionRenameUpdate>) {
				    DoTableRename(update, *temp_con_context);
			    }
		    },
		    transaction_update);
		DoSchemaDeletes(*temp_con_context);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		commit_state_unknown = CommitStateUnknown(error);
		CleanupFiles();
		temp_con.Rollback();
		EvictCachedTables();
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
		for (auto &entry : alter_update.updated_tables) {
			ic_catalog.table_request_cache.Expire(context, entry.first);
		}
	}
}

void IcebergTransaction::DoTableRename(IcebergTransactionRenameUpdate &rename_update, ClientContext &context) {
	auto &original_table = rename_update.table.get();
	auto &schema = original_table.schema;
	auto source_table_key = original_table.GetTableKey();
	auto &table_name = original_table.name;
	auto new_name = rename_update.new_name;
	auto &new_table = rename_update.new_table.get();
	auto destination_table_key = new_table.GetTableKey();

	rest_api_objects::RenameTableRequest request;
	request.source._namespace.value = schema.namespace_items;
	request.source.name = table_name;
	request.destination._namespace.value = schema.namespace_items;
	request.destination.name = new_name;
	auto transaction_json = RESTObjectToJSONString(request);
	IRCAPI::CommitTableRename(context, catalog, transaction_json);

	if (catalog.attach_options.max_table_staleness_micros.IsValid()) {
		//! The shared cache must only change once the catalog rename is durable.
		catalog.table_request_cache.Expire(context, source_table_key);
		catalog.table_request_cache.Expire(context, destination_table_key);
	}

	DropInfo drop_info;
	drop_info.GetQualifiedNameMutable() = Identifier(table_name);
	drop_info.if_not_found = OnEntryNotFound::THROW_EXCEPTION;
	schema.DropEntry(context, drop_info, true);

	lock_guard<mutex> guard(schema.tables.GetEntryLock());
	shared_ptr<IcebergTableInformation> old_version;
	schema.tables.CreateEntryInternal(guard, new_name, std::move(new_table), old_version);
	if (old_version) {
		throw TransactionException("Table %s was already created by a different transaction!", new_name);
	}
}

void IcebergTransaction::DoMultiTableCommitUpdates(IcebergTransactionAlterUpdate &alter_update,
                                                   ClientContext &context) {
	//! The retry policy is the tables' folded (most-lenient) config, produced by StageMultiTableCommit.
	//! It is stable across attempts (same tables), so the backoff state is created once, lazily, from
	//! the first staged request and reused for every retry.
	std::optional<IcebergRetryBackoff> backoff;
	for (idx_t attempt = 0;; attempt++) {
		auto transaction_info = StageMultiTableCommit(db, alter_update, context);
		if (transaction_info.request.table_changes.empty()) {
			alter_update.updated_tables.clear();
			return;
		}
		if (!backoff) {
			backoff.emplace(transaction_info.retry_config);
		}

		std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
		auto doc = doc_p.get();
		auto root_object = transaction_info.request.ToJSON(doc);
		yyjson_mut_doc_set_root(doc, root_object);

		auto transaction_json = JsonDocToString(std::move(doc_p));
		auto result = IRCAPI::CommitMultiTableUpdate(context, catalog, transaction_json);
		if (result.Success()) {
			return;
		}
		if (!CommitIsRetryable(transaction_info.retryable, transaction_info.retry_config.num_retries, result,
		                       attempt)) {
			result.Throw(catalog.GetBaseUrl().GetURLEncoded());
		}
		CleanupMetadataFiles(context, transaction_info.created_metadata_files);
		RefreshRetryTables(alter_update, transaction_info.table_keys, context);
		//! Back off before the next attempt to de-synchronize concurrent writers; stop if the
		//! cumulative retry budget (commit.retry.total-timeout-ms) would be exceeded.
		if (!backoff->WaitBeforeRetry(attempt)) {
			result.Throw(catalog.GetBaseUrl().GetURLEncoded());
		}
	}
}

void IcebergTransaction::DoSingleTableCommitUpdates(IcebergTransactionAlterUpdate &alter_update,
                                                    ClientContext &context) {
	D_ASSERT(catalog.supported_urls.count("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}"));
	auto table_info = GetSingleUpdatedTable(alter_update);
	if (!table_info) {
		return;
	}
	auto table_key = table_info->GetTableKey();
	//! Backoff state is created once (lazily, from the first staged commit's config) and reused
	//! across retries for this single atomic request.
	std::optional<IcebergRetryBackoff> backoff;
	for (idx_t attempt = 0;; attempt++) {
		auto table_transaction_info = StageSingleTableCommit(db, *table_info, context);
		if (!backoff) {
			backoff.emplace(table_transaction_info.retry_config);
		}
		auto &table_change = table_transaction_info.request;
		D_ASSERT(table_change.identifier);
		auto &identifier = *table_change.identifier;
		auto transaction_json = ConstructTableUpdateJSON(table_change);
		auto result =
		    IRCAPI::CommitTableUpdate(context, catalog, identifier._namespace.value, identifier.name, transaction_json);
		if (result.Success()) {
			return;
		}

		if (!CommitIsRetryable(table_transaction_info.retryable, table_transaction_info.retry_config.num_retries,
		                       result, attempt)) {
			result.Throw(catalog.GetBaseUrl().GetURLEncoded());
		}
		CleanupMetadataFiles(context, table_transaction_info.created_metadata_files);
		case_insensitive_set_t retry_tables;
		retry_tables.insert(table_key);
		RefreshRetryTables(alter_update, retry_tables, context);
		//! Back off before the next attempt; stop if the retry budget would be exceeded.
		if (!backoff->WaitBeforeRetry(attempt)) {
			result.Throw(catalog.GetBaseUrl().GetURLEncoded());
		}
	}
}

void IcebergTransaction::DoTableDeletes(IcebergTransactionDeleteUpdate &delete_update, ClientContext &context) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto &table = delete_update.deleted_table.get();
	auto schema_key = table.schema.name;
	auto table_key = table.GetTableKey();
	auto &table_name = table.name;
	IRCAPI::CommitTableDelete(context, catalog, table.schema.namespace_items, table_name);
	// remove the load table result
	ic_catalog.table_request_cache.Expire(context, table_key);
	// remove the table entry from the catalog
	auto &schema_entry = ic_catalog.schemas.GetEntry(schema_key.GetIdentifierName()).Cast<IcebergSchemaEntry>();
	DropInfo drop_info;
	drop_info.GetQualifiedNameMutable() = Identifier(table_name);
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
		auto result = connection.Query("COMMIT");
		if (result->HasError()) {
			connection.Query("ROLLBACK");
		}
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
	if (commit_state_unknown) {
		// Commit may have landed (CommitStateUnknownException); keep the files.
		return;
	}
	ScopedTransaction temp_con(db);
	auto &temp_context = temp_con.GetContext();
	auto &fs = FileSystem::GetFileSystem(temp_context);

	if (auto alter_update = GetAlterUpdate()) {
		for (auto &up_table : alter_update->updated_tables) {
			auto &table = up_table.second.get();
			if (!table.transaction_data) {
				// error occurred before transaction data was initialized
				// this can happen during table creation with table schema that cannot convert to
				// an iceberg table schema due to type incompatabilities
				continue;
			}
			auto &transaction_data = table.transaction_data;
			for (auto &update : transaction_data->updates) {
				if (update->type != IcebergTableUpdateType::ADD_SNAPSHOT) {
					continue;
				}
				// we need to recreate the keys in the current context.
				auto &ic_table_entry = table.GetLatestSchema()->Cast<IcebergTableEntry>();
				ic_table_entry.PrepareIcebergScanFromEntry(temp_context);

				auto &add_snapshot = update->Cast<IcebergAddSnapshot>();
				const auto manifest_list_entries = add_snapshot.GetManifestFiles();
				for (const auto &manifest : manifest_list_entries) {
					for (auto &manifest_entry : manifest.GetManifestEntries()) {
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

void IcebergTransaction::EvictCachedTables() {
	if (!catalog.attach_options.max_table_staleness_micros.IsValid()) {
		return;
	}
	ScopedTransaction temp_con(db);
	auto &temp_context = temp_con.GetContext();
	std::visit(
	    [&](auto &update) {
		    using T = std::decay_t<decltype(update)>;
		    if constexpr (std::is_same_v<T, IcebergTransactionAlterUpdate>) {
			    for (auto &up_table : update.updated_tables) {
				    catalog.table_request_cache.Expire(temp_context, up_table.first);
			    }
		    } else if constexpr (std::is_same_v<T, IcebergTransactionDeleteUpdate>) {
			    catalog.table_request_cache.Expire(temp_context, update.deleted_table.get().GetTableKey());
		    }
	    },
	    transaction_update);
}

void IcebergTransaction::Rollback() {
	CleanupFiles();
}

IcebergTransaction &IcebergTransaction::Get(ClientContext &context, Catalog &catalog) {
	D_ASSERT(catalog.GetCatalogType() == "iceberg");
	return Transaction::Get(context, catalog).Cast<IcebergTransaction>();
}

bool IcebergTransaction::StartedBefore(timestamp_ms_t timestamp_ms) const {
	auto ctx = context.lock();
	auto transaction_start_ms = IcebergUtils::GetTransactionStartTimeMS(*ctx);
	return transaction_start_ms < timestamp_ms;
}

optional_ptr<IcebergTransactionTableState> IcebergTransaction::GetLatestTableState(const string &table_key) {
	auto it = current_table_data.find(table_key);
	if (it == current_table_data.end()) {
		return nullptr;
	}
	return it->second;
}

IcebergTransactionTableState &IcebergTransaction::SetLatestTableState(const string &table_key,
                                                                      IcebergTableStatus status) {
	auto it = current_table_data.find(table_key);
	if (it == current_table_data.end()) {
		it = current_table_data.emplace(table_key, IcebergTransactionTableState()).first;
	}
	it->second.SetStatus(status);
	return it->second;
}

IcebergTransactionTableState &IcebergTransaction::SetCatalogTableState(shared_ptr<IcebergTableInformation> table) {
	auto table_key = table->GetTableKey();
	auto result = current_table_data.emplace(table_key, IcebergTransactionTableState(std::move(table)));
	return result.first->second;
}

IcebergTransactionTableState &IcebergTransaction::SetTransactionTableState(const string &table_key,
                                                                           IcebergTableInformation &&table,
                                                                           IcebergTableStatus status) {
	auto it = current_table_data.find(table_key);
	if (it == current_table_data.end()) {
		it = current_table_data.emplace(table_key, IcebergTransactionTableState(std::move(table))).first;
	} else {
		if (!it->second.IsMissing()) {
			throw InternalException("Transaction state already exists for table '%s'", table_key);
		}
		it->second = IcebergTransactionTableState(std::move(table));
	}
	it->second.SetStatus(status);
	return it->second;
}

IcebergTransactionTableState &
IcebergTransaction::GetOrCreateTransactionTableState(const IcebergTableInformation &table) {
	auto table_key = table.GetTableKey();
	auto state = GetLatestTableState(table_key);
	if (state) {
		return *state;
	}
	auto copy = table.Copy(*this);
	return SetTransactionTableState(table_key, std::move(copy), IcebergTableStatus::ALIVE);
}

IcebergTransactionAlterUpdate &IcebergTransaction::GetOrCreateAlter() {
	if (!HasTableUpdate()) {
		transaction_update.emplace<IcebergTransactionAlterUpdate>(*this);
	}
	auto alter_update = GetAlterUpdate();
	if (!alter_update) {
		throw TransactionException("Iceberg REST Catalog cannot commit this transaction atomically because it mixes "
		                           "table updates with rename/drop requests");
	}
	return *alter_update;
}

IcebergTableInformation &IcebergTransaction::DeleteTable(IcebergTableInformation &table) {
	auto table_key = table.GetTableKey();
	auto state = GetLatestTableState(table_key);
	if (HasTableUpdate()) {
		throw TransactionException("Iceberg REST Catalog cannot commit this transaction atomically because it mixes "
		                           "table updates with rename/drop requests");
	}

	if (!state) {
		state = GetOrCreateTransactionTableState(table);
	}
	auto &deleted_table = state->GetOrCreateTransactionInfo(*this);
	state->SetStatus(IcebergTableStatus::DROPPED);
	transaction_update.emplace<IcebergTransactionDeleteUpdate>(*this, deleted_table);
	return state->GetInfo();
}

IcebergTableInformation &IcebergTransaction::RenameTable(IcebergTableInformation &table, const string &new_name) {
	auto table_key = table.GetTableKey();
	auto state = GetLatestTableState(table_key);
	if (HasTableUpdate()) {
		throw TransactionException("Iceberg REST Catalog cannot commit this transaction atomically because it mixes "
		                           "table updates with rename/drop requests");
	}

	if (!state) {
		state = GetOrCreateTransactionTableState(table);
	}
	state->SetStatus(IcebergTableStatus::RENAMED);
	auto &source_table = state->GetInfo();
	auto new_table = source_table.Copy();
	new_table.name = new_name;
	auto new_table_key = new_table.GetTableKey();
	auto &new_state = SetTransactionTableState(new_table_key, std::move(new_table), IcebergTableStatus::ALIVE);

	//! Create the rename update, creating the new IcebergTableInformation in the process
	transaction_update.emplace<IcebergTransactionRenameUpdate>(*this, source_table, new_state.GetInfo(), new_name);
	return state->GetInfo();
}

void ApplyTableUpdate(IcebergTableInformation &table_info, IcebergTransaction &iceberg_transaction,
                      const std::function<void(IcebergTableInformation &)> &callback) {
	auto &alter = iceberg_transaction.GetOrCreateAlter();
	auto &updated_table = alter.GetOrInitializeTable(table_info);
	callback(updated_table);
}

} // namespace duckdb
