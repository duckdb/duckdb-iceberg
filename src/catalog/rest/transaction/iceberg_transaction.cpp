#include "catalog/rest/transaction/iceberg_transaction.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/storage/table/update_state.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/common/json_document.hpp"

#include <chrono>
#include <optional>
#include <random>
#include <thread>

#include "common/iceberg_constants.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/api/iceberg_retry.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/transaction/iceberg_commit_plan.hpp"
#include "catalog/rest/iceberg_table_prefetch.hpp"
#include "catalog/rest/storage/iceberg_authorization.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/api/iceberg_add_snapshot.hpp"
#include "catalog/rest/api/iceberg_create_table_request.hpp"
#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/api/iceberg_type.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "iceberg_logging.hpp"
#include "catalog/rest/api/table_update.hpp"
#include "catalog/rest/transaction/iceberg_transaction_update.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {

IcebergTransactionTableState::IcebergTransactionTableState() : status(IcebergTableStatus::MISSING) {
}

IcebergTransactionTableState::IcebergTransactionTableState(shared_ptr<IcebergTable> catalog_table)
    : catalog_table(std::move(catalog_table)), status(IcebergTableStatus::ALIVE) {
}

IcebergTransactionTableState::IcebergTransactionTableState(IcebergTable &&transaction_table_p)
    : transaction_table(make_uniq<IcebergTable>(std::move(transaction_table_p))), status(IcebergTableStatus::ALIVE) {
	if (!transaction_table->GetMetadata().GetSchemas().IsEmpty()) {
		transaction_table->InitSchemaVersions();
	}
}

const IcebergTable &IcebergTransactionTableState::GetInfo() const {
	return const_cast<IcebergTransactionTableState &>(*this).GetInfo();
}

IcebergTable &IcebergTransactionTableState::GetOrCreateTransactionInfo(IcebergTransaction &transaction) {
	if (transaction_table) {
		return *transaction_table;
	}
	if (!catalog_table) {
		throw InternalException("Cannot materialize transaction table state without table information");
	}
	transaction_table = make_uniq<IcebergTable>(catalog_table->Copy(transaction));
	transaction_table->InitSchemaVersions();
	return *transaction_table;
}

IcebergTransaction::IcebergTransaction(IcebergCatalog &ic_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), db(*context.db), catalog(ic_catalog), access_mode(ic_catalog.access_mode),
      scan_serialization_id(UUID::GenerateRandomUUID()) {
}

IcebergTransaction::~IcebergTransaction() = default;

void IcebergTransaction::Start() {
}

IcebergCatalog &IcebergTransaction::GetCatalog() {
	return catalog;
}

IcebergTablePrefetch &IcebergTransaction::GetTablePrefetch(ClientContext &context) {
	if (!table_prefetch) {
		table_prefetch = make_uniq<IcebergTablePrefetch>(context, catalog);
	}
	return *table_prefetch;
}

optional_ptr<IcebergTablePrefetch> IcebergTransaction::TryGetTablePrefetch() {
	return table_prefetch.get();
}

bool IcebergTransaction::CanUseMultiTableCommit(const IcebergTransactionAlterUpdate &alter_update) const {
	auto &capabilities = catalog.GetBackend().Capabilities();
	if (!capabilities.atomic_multi_table_commit) {
		return false;
	}
	if (capabilities.atomic_commit_with_creates) {
		return true;
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
	throw TransactionException("Iceberg %s cannot commit this transaction atomically because it would "
	                           "require multiple table commit requests without atomic multi-table commit support",
	                           catalog.GetBackend().Name());
}

bool IcebergTransaction::MultiTableCommitAvailable() const {
	return catalog.GetBackend().Capabilities().atomic_multi_table_commit;
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

namespace {

void RemoveFilesBestEffort(ClientContext &context, FileSystem &fs, const vector<string> &paths,
                           const char *description) {
	if (paths.empty()) {
		return;
	}
	try {
		fs.RemoveFiles(paths);
		DUCKDB_LOG(context, IcebergLogType, "Iceberg Transaction Cleanup, deleted %llu %s(s)",
		           static_cast<uint64_t>(paths.size()), description);
	} catch (std::exception &ex) {
		//! A bulk delete reports one error for the whole batch, so some of these files may well have been removed.
		DUCKDB_LOG(context, IcebergLogType,
		           "Iceberg Transaction Cleanup, failed to delete one or more of %llu %s(s): %s",
		           static_cast<uint64_t>(paths.size()), description, ex.what());
	}
}

} // namespace

void IcebergTransaction::CleanupMetadataFiles(ClientContext &context, const vector<string> &paths) {
	if (!catalog.GetBackend().Capabilities().delete_written_files || paths.empty()) {
		return;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	unordered_set<string> seen;
	vector<string> unique_paths;
	unique_paths.reserve(paths.size());
	for (const auto &path : paths) {
		if (seen.insert(path).second) {
			unique_paths.push_back(path);
		}
	}
	RemoveFilesBestEffort(context, fs, unique_paths, "metadata file");
}

void IcebergTransaction::RefreshRetryTables(IcebergTransactionAlterUpdate &alter_update,
                                            const unordered_set<string> &table_keys, ClientContext &context) {
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

void IcebergTransaction::Commit() {
	if (!HasTableUpdate() && created_schemas.Empty() && deleted_schemas.empty() && schema_property_updates.empty() &&
	    created_views.empty() && deleted_views.empty()) {
		// Read-only transactions have no catalog commit work; temporary vended storage secrets
		// are left to transaction/session cleanup.
		return;
	}
	// Unless the backend publishes everything together, views are not part of the atomic table commit. Validate
	// the entire write set before any schema, table, or view request can reach the catalog.
	auto &backend = catalog.GetBackend();
	auto view_operations = created_views.size() + deleted_views.size();
	if (!backend.Capabilities().atomic_catalog_commit && view_operations &&
	    (view_operations > 1 || HasTableUpdate() || !created_schemas.Empty() || !deleted_schemas.empty() ||
	     !schema_property_updates.empty())) {
		CleanupFiles();
		DropCreatedTables();
		throw TransactionException("Iceberg %s cannot commit view changes atomically with other catalog "
		                           "changes; commit each view create or drop separately",
		                           backend.Name());
	}

	Connection temp_con(db);
	temp_con.BeginTransaction();
	auto &temp_con_context = temp_con.context;

	// Copy user settings from the original context so that e.g. s3_access_key_id are available
	if (!this->context.expired()) {
		temp_con_context->config = this->context.lock()->config;
	}

	try {
		IcebergCommitPlan plan(*this);
		backend.Commit(*temp_con_context, plan);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		CleanupFiles();
		DropCreatedTables();
		temp_con.Rollback();
		EvictCachedTables();
		error.Throw("Failed to commit Iceberg transaction: ");
	}

	temp_con.Rollback();
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

void IcebergTransaction::InvalidateViewEntry(const string &view_key) {
	auto entry = views.find(view_key);
	if (entry != views.end()) {
		retired_views.push_back(std::move(entry->second));
		views.erase(entry);
	}
}

void IcebergTransaction::CleanupFiles() {
	// remove any files that were written
	if (!catalog.GetBackend().Capabilities().delete_written_files) {
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
			//! Batched per table: the keys PrepareIcebergScanFromEntry registers below belong to this table, so
			//! the files have to be deleted before moving on to the next one.
			vector<string> data_files;
			unordered_set<string> seen_paths;
			for (auto &update : transaction_data->updates) {
				if (update->type != IcebergTableUpdateType::ADD_SNAPSHOT) {
					continue;
				}
				// we need to recreate the keys in the current context.
				auto &ic_table_entry = table.GetLatestSchema()->Cast<IcebergTableSchemaVersion>();
				ic_table_entry.PrepareIcebergScanFromEntry(temp_context);

				auto &add_snapshot = update->Cast<IcebergAddSnapshot>();
				for (const auto &manifest : add_snapshot.GetPendingManifests()) {
					for (const auto &manifest_entry : manifest.GetEntries()) {
						// Several deletion-vector entries can reference the same physical Puffin file.
						const auto &path = manifest_entry.data_file.file_path;
						if (seen_paths.insert(path).second) {
							data_files.push_back(path);
						}
					}
				}
			}
			RemoveFilesBestEffort(temp_context, fs, data_files, "data_file");
		}
	}
}

void IcebergTransaction::EvictCachedTables() {
	auto &backend = catalog.GetBackend();
	auto evict = [&](const IcebergTable &table) {
		backend.OnTableChanged(table.GetTableKey(), table.initialization_source.get(), false);
	};
	std::visit(
	    [&](auto &update) {
		    using T = std::decay_t<decltype(update)>;
		    if constexpr (std::is_same_v<T, IcebergTransactionAlterUpdate>) {
			    for (auto &up_table : update.updated_tables) {
				    evict(up_table.second.get());
			    }
		    } else if constexpr (std::is_same_v<T, IcebergTransactionDeleteUpdate>) {
			    evict(update.deleted_table.get());
		    }
	    },
	    transaction_update);
}

void IcebergTransaction::TableCreatedInCatalog(const string &table_key, IcebergEntryId id,
                                               shared_ptr<const IcebergLoadedTable> created) {
	created_tables.push_back({table_key, std::move(id), std::move(created)});
}

void IcebergTransaction::DropCreatedTables() {
	if (created_tables.empty()) {
		return;
	}
	if (commit_state_unknown) {
		// Commit may have landed (CommitStateUnknownException); keep the tables.
		return;
	}
	ScopedTransaction temp_con(db);
	auto &temp_context = temp_con.GetContext();
	auto &backend = catalog.GetBackend();
	for (auto &table : created_tables) {
		auto state = GetLatestTableState(table.table_key);
		if (state && state->IsAlive()) {
			auto &created_table = state->GetInfo();
			backend.OnTableChanged(created_table.GetTableKey(), created_table.initialization_source.get(), true);
		}
	}
	IcebergCommitPlan plan(*this);
	backend.Abort(temp_context, plan);
	created_tables.clear();
}

void IcebergTransaction::Rollback() {
	CleanupFiles();
	DropCreatedTables();
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

IcebergTransactionTableState &IcebergTransaction::SetCatalogTableState(shared_ptr<IcebergTable> table) {
	auto table_key = table->GetTableKey();
	auto result = current_table_data.emplace(table_key, IcebergTransactionTableState(std::move(table)));
	return result.first->second;
}

IcebergTransactionTableState &
IcebergTransaction::SetTransactionTableState(const string &table_key, IcebergTable &&table, IcebergTableStatus status) {
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

IcebergTransactionTableState &IcebergTransaction::GetOrCreateTransactionTableState(const IcebergTable &table) {
	auto table_key = table.GetTableKey();
	auto state = GetLatestTableState(table_key);
	if (state) {
		return *state;
	}
	auto copy = table.Copy(*this);
	return SetTransactionTableState(table_key, std::move(copy), IcebergTableStatus::ALIVE);
}

void IcebergTransaction::ThrowMixedTableChanges() const {
	//! A transaction makes one kind of table change (the IcebergTransactionUpdate variant), whatever the backend
	throw TransactionException("Iceberg %s cannot commit this transaction atomically because it mixes "
	                           "table updates with rename/drop requests",
	                           catalog.GetBackend().Name());
}

IcebergTransactionAlterUpdate &IcebergTransaction::GetOrCreateAlter() {
	if (!HasTableUpdate()) {
		transaction_update.emplace<IcebergTransactionAlterUpdate>(*this);
	}
	auto alter_update = GetAlterUpdate();
	if (!alter_update) {
		ThrowMixedTableChanges();
	}
	return *alter_update;
}

IcebergTable &IcebergTransaction::DeleteTable(IcebergTable &table) {
	auto table_key = table.GetTableKey();
	auto state = GetLatestTableState(table_key);
	if (HasTableUpdate()) {
		ThrowMixedTableChanges();
	}

	if (!state) {
		state = GetOrCreateTransactionTableState(table);
	}
	auto &deleted_table = state->GetOrCreateTransactionInfo(*this);
	state->SetStatus(IcebergTableStatus::DROPPED);
	transaction_update.emplace<IcebergTransactionDeleteUpdate>(*this, deleted_table);
	return state->GetInfo();
}

IcebergTable &IcebergTransaction::RenameTable(IcebergTable &table, const string &new_name) {
	auto table_key = table.GetTableKey();
	auto state = GetLatestTableState(table_key);
	if (HasTableUpdate()) {
		ThrowMixedTableChanges();
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

	//! Create the rename update, creating the new IcebergTable in the process
	transaction_update.emplace<IcebergTransactionRenameUpdate>(*this, source_table, new_state.GetInfo(), new_name);
	return state->GetInfo();
}

void ApplyTableUpdate(unique_lock<mutex> &table_update_guard, IcebergTable &table_info,
                      IcebergTransaction &iceberg_transaction, const std::function<void(IcebergTable &)> &callback) {
	D_ASSERT(table_update_guard.owns_lock() && table_update_guard.mutex() == &iceberg_transaction.table_update_lock);
	auto &alter = iceberg_transaction.GetOrCreateAlter();
	auto &updated_table = alter.GetOrInitializeTable(table_info);
	callback(updated_table);
}

void ApplyTableUpdate(IcebergTable &table_info, IcebergTransaction &iceberg_transaction,
                      const std::function<void(IcebergTable &)> &callback) {
	//! Several operators or table functions of one statement can change tables of this transaction concurrently
	unique_lock<mutex> guard(iceberg_transaction.table_update_lock);
	ApplyTableUpdate(guard, table_info, iceberg_transaction, callback);
}

} // namespace duckdb
