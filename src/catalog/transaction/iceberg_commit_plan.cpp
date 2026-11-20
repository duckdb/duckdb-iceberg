#include "catalog/transaction/iceberg_commit_plan.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

#include <chrono>
#include <optional>
#include <random>
#include <thread>

#include "catalog/rest/api/iceberg_add_snapshot.hpp"
#include "catalog/rest/api/iceberg_table_requirement.hpp"
#include "catalog/rest/api/table_update.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/transaction/iceberg_transaction_update.hpp"
#include "common/iceberg_constants.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"

namespace duckdb {

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

static bool NeedsAssertSchemaId(const IcebergTransactionData &transaction_data, const IcebergTable &table_info) {
	(void)table_info;
	return transaction_data.assert_schema_id;
}

namespace {

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
	if (transaction_data.replaces_table) {
		AssertRefSnapshotId requirement(transaction_data.initial_snapshot_id);
		requirement.CreateRequirement(db, context, commit_state);
	} else if (current_snapshot && !transaction_data.alters.empty()) {
		commit_state.table_change.requirements.push_back(CreateAssertRefSnapshotIdRequirement(*current_snapshot));
	} else if (!current_snapshot && !transaction_data.alters.empty() && !has_assert_create) {
		commit_state.table_change.requirements.push_back(CreateAssertNoSnapshotRequirement());
	}
}

//! Safe to re-apply a DELETE on retry only if every commit in (base, tip] was a
//! pure append and the tip descends from the scanned snapshot. Keep the sequence
//! range validation, then verify ancestry to reject replacement lineage breaks.
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
	// Replacement can publish a newer append with no parent. Sequence order alone
	// does not prove that the tip continues the history the DELETE scanned.
	unordered_set<int64_t> visited;
	auto ancestor = tip;
	while (ancestor->snapshot_id != base_snapshot_id) {
		if (!ancestor->parent_snapshot_id || !visited.insert(ancestor->snapshot_id).second) {
			return false;
		}
		ancestor = metadata.FindSnapshotByIdInternal(*ancestor->parent_snapshot_id);
		if (!ancestor) {
			return false;
		}
	}
	return true;
}

//! Throw if a retried DELETE can't be safely re-applied. No-op on the first
//! attempt (tip == scan) and for non-delete transactions.
static void VerifyDeleteRetryability(const IcebergTable &table_info,
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
	auto scan_snapshot_id = *transaction_data.base_snapshot_id;
	auto tip_snapshot_id = current_snapshot->snapshot_id;

	//! Nothing to reconcile if the tip hasn't moved since we scanned (e.g. the first attempt).
	if (scan_snapshot_id == tip_snapshot_id) {
		return;
	}

	//! Re-applying a DELETE over concurrent commits is opt-in: the table must explicitly request
	//! 'snapshot' isolation, and even then only a pure-append history is safe (DeleteCanReapply). Any
	//! other isolation - unset (Iceberg's default is 'serializable') or an explicit 'serializable' - must
	//! abort, since a concurrent append can add rows matching the delete predicate that a silent re-apply
	//! would leave behind.
	auto isolation_level = table_info.GetMetadata().GetTableProperty(WRITE_DELETE_ISOLATION_LEVEL);
	if (StringUtil::CIEquals(isolation_level, "snapshot") &&
	    DeleteCanReapply(table_info.GetMetadata(), scan_snapshot_id, tip_snapshot_id)) {
		return;
	}
	throw TransactionException(
	    "DELETE on \"%s\" conflicts with a concurrent commit (scanned snapshot %s, now at %s); re-run the DELETE. "
	    "Set 'write.delete.isolation-level'='snapshot' to allow re-applying deletes over concurrent appends.",
	    table_info.name, std::to_string(scan_snapshot_id), std::to_string(tip_snapshot_id));
}

//! Stages one table: writes its manifests and manifest list and builds its requirements and updates
static void StageSingleTableCommit(DatabaseInstance &db, IcebergTable &table_info, ClientContext &context,
                                   IcebergStagedCommit &info) {
	IcebergCommitState commit_state(table_info, context);
	auto &table_change = commit_state.table_change;
	auto &schema = table_info.schema.Cast<IcebergSchemaEntry>();
	table_change.identifier = rest_api_objects::TableIdentifier();
	table_change.identifier->_namespace.value = schema.namespace_items;
	table_change.identifier->name = table_info.name;

	auto &metadata = commit_state.table_info.GetMetadata();
	auto current_snapshot = metadata.GetLatestSnapshot();
	auto &transaction_data = *commit_state.table_info.transaction_data;
	transaction_data.VerifyMergeDeletesComplete();
	info.retryable = transaction_data.SupportsAppendRetry();
	info.retry_config = IcebergRetryConfig::FromTableMetadata(metadata);
	if (!transaction_data.alters.empty()) {
		table_info.LoadCredentials(context);
		commit_state.LoadExistingManifests(db, std::move(transaction_data.existing_manifest_list));
	}
	VerifyDeleteRetryability(commit_state.table_info, current_snapshot);

	for (auto &update : transaction_data.updates) {
		update->CreateUpdate(db, context, commit_state);
	}

	CreateTableRequirements(db, context, commit_state, transaction_data, current_snapshot);

	if (!transaction_data.alters.empty()) {
		auto &snapshot = *commit_state.GetLatestSnapshot();
		SetSnapshotRef set_snapshot_ref(snapshot.snapshot_id);
		set_snapshot_ref.CreateUpdate(db, context, commit_state);
	}

	info.written_files = std::move(commit_state.created_metadata_files);
	info.changes.push_back(std::move(table_change));
	info.table_keys.insert(table_info.GetTableKey());
}

static void StageMultiTableCommit(DatabaseInstance &db, IcebergTransactionAlterUpdate &alter_update,
                                  ClientContext &context, IcebergStagedCommit &info) {
	bool all_retryable = true;
	bool saw_table = false;
	for (auto &updated_table : alter_update.updated_tables) {
		auto &table_info = updated_table.second.get();
		if (!table_info.HasTransactionUpdates()) {
			//! No changes to commit
			continue;
		}

		IcebergStagedCommit table_transaction_info;
		StageSingleTableCommit(db, table_info, context, table_transaction_info);
		info.written_files.insert(info.written_files.end(), table_transaction_info.written_files.begin(),
		                          table_transaction_info.written_files.end());
		info.table_keys.insert(updated_table.first);
		info.changes.push_back(std::move(table_transaction_info.changes[0]));
		//! Tables in one atomic transaction share a single retry loop, so fold their retry policies
		//! into the most lenient one (first table seeds it, the rest are merged in).
		info.retry_config = saw_table ? info.retry_config.MostLenient(table_transaction_info.retry_config)
		                              : table_transaction_info.retry_config;
		saw_table = true;
		all_retryable = all_retryable && table_transaction_info.retryable;
	}
	info.retryable = saw_table && all_retryable;
}

static optional_ptr<IcebergTable> GetSingleUpdatedTable(IcebergTransactionAlterUpdate &alter_update) {
	optional_ptr<IcebergTable> result;
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

//===--------------------------------------------------------------------===//
// IcebergCommitPlan
//===--------------------------------------------------------------------===//

IcebergCommitPlan::IcebergCommitPlan(IcebergTransaction &transaction) : transaction(transaction) {
	auto &backend = transaction.GetCatalog().GetBackend();
	for (auto &created_schema : transaction.created_schemas) {
		created_namespace_names.push_back(created_schema.first);
		created_namespaces.push_back(backend.ParseNamespace(created_schema.first));
	}
	for (auto &schema_name : transaction.deleted_schemas) {
		dropped_namespace_names.push_back(schema_name);
		dropped_namespaces.push_back(backend.ParseNamespace(schema_name));
	}
}

void IcebergCommitPlan::OnNamespaceCreated(idx_t index, bool already_existed) {
	auto &schema_name = created_namespace_names[index];
	if (already_existed) {
		transaction.created_schemas.OnNamespaceExists(schema_name);
	}
	for (auto &created_schema : transaction.created_schemas) {
		if (created_schema.first == schema_name) {
			transaction.GetCatalog().GetSchemas().AddEntry(schema_name, created_schema.second.entry);
			return;
		}
	}
	throw InternalException("Created namespace '%s' is not part of the transaction", schema_name);
}

vector<IcebergNamespacePropertyUpdate> IcebergCommitPlan::NamespacePropertyUpdates() const {
	auto &backend = transaction.GetCatalog().GetBackend();
	vector<IcebergNamespacePropertyUpdate> result;
	for (auto &properties_update : transaction.schema_property_updates) {
		auto schema_name_with_catalog = properties_update.first;
		auto catalog_splitter = schema_name_with_catalog.find(".");
		auto schema_name_no_catalog = schema_name_with_catalog.erase(0, catalog_splitter + 1);

		auto &schema_property_updates = properties_update.second;
		IcebergNamespacePropertyUpdate update;
		update.ns = backend.ParseNamespace(schema_name_no_catalog);
		update.removals.assign(schema_property_updates.removals.begin(), schema_property_updates.removals.end());
		update.updates = schema_property_updates.updates;
		result.push_back(std::move(update));
	}
	return result;
}

void IcebergCommitPlan::OnNamespaceDropped(idx_t index) {
	transaction.GetCatalog().GetSchemas().RemoveEntry(dropped_namespace_names[index]);
}

void IcebergCommitPlan::OnNamespacesDropped() {
	transaction.deleted_schemas.clear();
}

IcebergTableChangeKind IcebergCommitPlan::TableChangeKind() const {
	return std::visit(
	    [](auto &update) {
		    using T = std::decay_t<decltype(update)>;
		    if constexpr (std::is_same_v<T, IcebergTransactionAlterUpdate>) {
			    return IcebergTableChangeKind::UPDATE;
		    } else if constexpr (std::is_same_v<T, IcebergTransactionDeleteUpdate>) {
			    return IcebergTableChangeKind::DROP;
		    } else if constexpr (std::is_same_v<T, IcebergTransactionRenameUpdate>) {
			    return IcebergTableChangeKind::RENAME;
		    } else {
			    return IcebergTableChangeKind::NONE;
		    }
	    },
	    transaction.transaction_update);
}

bool IcebergCommitPlan::HasTableUpdates() const {
	auto alter_update = transaction.GetAlterUpdate();
	return alter_update && alter_update->HasUpdates();
}

bool IcebergCommitPlan::CanCommitTablesAtomically() const {
	return transaction.CanUseMultiTableCommit(*transaction.GetAlterUpdate());
}

bool IcebergCommitPlan::StageTables(ClientContext &context, IcebergStagedCommit &staged) {
	auto &alter_update = *transaction.GetAlterUpdate();
	StageMultiTableCommit(transaction.db, alter_update, context, staged);
	if (staged.changes.empty()) {
		alter_update.updated_tables.clear();
		return false;
	}
	return true;
}

bool IcebergCommitPlan::StageTable(ClientContext &context, IcebergStagedCommit &staged) {
	if (!single_table_resolved) {
		single_table = GetSingleUpdatedTable(*transaction.GetAlterUpdate());
		single_table_resolved = true;
	}
	if (!single_table) {
		return false;
	}
	StageSingleTableCommit(transaction.db, *single_table, context, staged);
	return true;
}

void IcebergCommitPlan::OnTablesUpdated() {
	auto &backend = transaction.GetCatalog().GetBackend();
	for (auto &entry : transaction.GetAlterUpdate()->updated_tables) {
		auto &table = entry.second.get();
		backend.OnTableChanged(table.GetTableKey(), table.initialization_source.get(), false);
	}
}

static IcebergEntryId GetEntryId(const IcebergTable &table) {
	return IcebergEntryId(IcebergNamespace(table.schema.namespace_items), table.name);
}

IcebergEntryId IcebergCommitPlan::DroppedTable() const {
	return GetEntryId(std::get<IcebergTransactionDeleteUpdate>(transaction.transaction_update).deleted_table.get());
}

void IcebergCommitPlan::OnTableDropped(ClientContext &context) {
	auto &table = std::get<IcebergTransactionDeleteUpdate>(transaction.transaction_update).deleted_table.get();
	// remove the load table result
	transaction.GetCatalog().GetBackend().OnTableChanged(table.GetTableKey(), table.initialization_source.get(), true);
	// remove the table entry from the catalog
	DropInfo drop_info;
	drop_info.type = CatalogType::TABLE_ENTRY;
	drop_info.GetQualifiedNameMutable() = Identifier(table.name);
	drop_info.if_not_found = OnEntryNotFound::RETURN_NULL;
	table.schema.DropEntry(context, drop_info, true);
}

IcebergEntryId IcebergCommitPlan::RenamedTable() const {
	return GetEntryId(std::get<IcebergTransactionRenameUpdate>(transaction.transaction_update).table.get());
}

string IcebergCommitPlan::NewTableName() const {
	return std::get<IcebergTransactionRenameUpdate>(transaction.transaction_update).new_name;
}

void IcebergCommitPlan::OnTableRenamed() {
	auto &rename_update = std::get<IcebergTransactionRenameUpdate>(transaction.transaction_update);
	auto &original_table = rename_update.table.get();
	auto &schema = original_table.schema;
	auto &table_name = original_table.name;
	auto new_name = rename_update.new_name;
	auto &new_table = rename_update.new_table.get();

	//! The shared cache must only change once the catalog rename is durable.
	auto &backend = transaction.GetCatalog().GetBackend();
	backend.OnTableChanged(original_table.GetTableKey(), original_table.initialization_source.get(), false);
	backend.OnTableChanged(new_table.GetTableKey(), new_table.initialization_source.get(), false);

	schema.tables.RenameEntry(table_name, new_name, std::move(new_table));
}

const vector<IcebergCreatedTableRecord> &IcebergCommitPlan::CreatedTables() const {
	return transaction.created_tables;
}

vector<IcebergEntryId> IcebergCommitPlan::DroppedViews() const {
	vector<IcebergEntryId> result;
	for (auto &deleted_view : transaction.deleted_views) {
		auto &info = deleted_view.second;
		result.emplace_back(IcebergNamespace(info.namespace_items), info.view_name);
	}
	return result;
}

void IcebergCommitPlan::OnViewsDropped() {
	transaction.deleted_views.clear();
}

vector<IcebergNewView> IcebergCommitPlan::CreatedViews(ClientContext &context) const {
	vector<IcebergNewView> result;
	auto &ic_catalog = transaction.GetCatalog();
	for (auto &view_entry : transaction.created_views) {
		auto &view_info = view_entry.second;
		auto &view_name = view_info->GetViewName().GetIdentifierName();
		// Look up the schema entry from the catalog by name
		auto &schema_entry = ic_catalog.schemas
		                         .GetEntry(context, view_info->GetQualifiedName().Schema().GetIdentifierName(),
		                                   OnEntryNotFound::THROW_EXCEPTION)
		                         ->Cast<IcebergSchemaEntry>();

		// query may have been moved by ViewCatalogEntry::Initialize(), so use sql field or reconstruct
		string view_sql;
		if (!view_info->sql.empty()) {
			view_sql = view_info->sql;
		} else if (view_info->query) {
			view_sql = view_info->query->ToString();
		} else {
			throw InternalException("Cannot commit view '%s': no SQL representation available", view_name);
		}

		// the view's output columns
		auto column_names = view_info->names;
		for (idx_t i = 0; i < view_info->aliases.size(); i++) {
			if (!view_info->aliases[i].empty()) {
				column_names[i] = view_info->aliases[i];
			}
		}
		// Iceberg schemas require unique field names, just like a bound DuckDB view.
		QueryResult::DeduplicateColumns(column_names);

		IcebergNamespace ns(schema_entry.namespace_items);
		IcebergNewView new_view {IcebergEntryId(ns, view_name), std::move(view_sql), {}, view_info->types, ns};
		for (idx_t i = 0; i < view_info->types.size(); i++) {
			new_view.column_names.push_back(column_names[i].GetIdentifierName());
		}
		result.push_back(std::move(new_view));
	}
	return result;
}

void IcebergCommitPlan::OnViewsCreated() {
	transaction.created_views.clear();
}

void IcebergCommitPlan::RemoveWrittenFiles(ClientContext &context, const vector<string> &paths) {
	transaction.CleanupMetadataFiles(context, paths);
}

void IcebergCommitPlan::RefreshForRetry(ClientContext &context, const unordered_set<string> &table_keys) {
	transaction.RefreshRetryTables(*transaction.GetAlterUpdate(), table_keys, context);
}

void IcebergCommitPlan::MarkOutcomeUnknown() {
	transaction.commit_state_unknown = true;
}

//===--------------------------------------------------------------------===//
// IcebergCommitRetryLoop
//===--------------------------------------------------------------------===//

static bool CommitIsRetryable(const IcebergStagedCommit &staged, const IcebergCatalogError &error, idx_t attempt) {
	if (attempt >= staged.retry_config.num_retries) {
		//! We've reached the max amount of retries
		return false;
	}
	if (!staged.retryable) {
		//! The operation isn't retryable in general
		return false;
	}
	if (error.status != IcebergCatalogStatus::CONFLICT) {
		//! Only conflicts are retryable: nothing was published
		return false;
	}
	return true;
}

void IcebergCommitRetryLoop::Run(ClientContext &context, IcebergCommitPlan &plan, const StageFunction &stage,
                                 const PublishFunction &publish) {
	//! The retry policy is stable across attempts (same tables), so the backoff state is created once, lazily, from
	//! the first staged attempt and reused for every retry.
	std::optional<IcebergRetryBackoff> backoff;
	for (idx_t attempt = 0;; attempt++) {
		IcebergStagedCommit staged;
		if (!stage(staged)) {
			return;
		}
		if (!backoff) {
			backoff.emplace(staged.retry_config);
		}
		auto error = publish(staged);
		if (!error) {
			return;
		}
		if (!CommitIsRetryable(staged, *error, attempt)) {
			error->Throw();
		}
		plan.RemoveWrittenFiles(context, staged.written_files);
		plan.RefreshForRetry(context, staged.table_keys);
		//! Back off before the next attempt to de-synchronize concurrent writers; stop if the
		//! cumulative retry budget (commit.retry.total-timeout-ms) would be exceeded.
		if (!backoff->WaitBeforeRetry(attempt)) {
			error->Throw();
		}
	}
}

} // namespace duckdb
