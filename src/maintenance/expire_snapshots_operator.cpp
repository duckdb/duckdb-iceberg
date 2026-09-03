#include "maintenance/expire_snapshots_operator.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "maintenance/maintenance_table_loader.hpp"

namespace duckdb {

namespace {

constexpr const char *EXPIRE_SNAPSHOTS_NAME = "iceberg_expire_snapshots";

vector<LogicalType> ExpireSnapshotsResultTypes() {
	return {LogicalType::BIGINT, LogicalType::BIGINT};
}

IcebergTable &LoadTransactionVisibleTable(ClientContext &context, const QualifiedName &table_name,
                                          IcebergCatalog &catalog, IcebergTransaction &transaction) {
	auto namespace_items = IRCAPI::ParseSchemaName(table_name.Schema().GetIdentifierName());
	auto table_key = IcebergTable::GetTableKey(catalog, namespace_items, table_name.Name().GetIdentifierName());
	auto transaction_state = transaction.GetLatestTableState(table_key);
	if (!transaction_state) {
		//! The first expiration access bypasses MAX_TABLE_STALENESS. Pin that
		//! result even for a no-op so later calls do not mix in catalog state.
		auto catalog_table = ReloadIcebergTableShared(context, table_name, EXPIRE_SNAPSHOTS_NAME, true);
		transaction_state = transaction.SetCatalogTableState(std::move(catalog_table));
		//! Materialize the transaction-visible copy before planning. This honors
		//! metadata-log time travel and guarantees staging reuses the same object.
		transaction_state->GetOrCreateTransactionInfo(transaction);
		transaction_state->MarkExpirationMetadataInitialized();
	}
	if (!transaction_state->IsAlive()) {
		throw TransactionException("Iceberg table '%s' was dropped or renamed in this transaction",
		                           table_name.Name().GetIdentifierName());
	}
	if (!transaction_state->IsExpirationMetadataInitialized()) {
		auto &table = transaction_state->GetInfo();
		if (table.transaction_data) {
			table.transaction_data->VerifySnapshotExpirationAllowed();
		}
		throw TransactionException(
		    "%s: table '%s' was already accessed in this transaction; run expiration in a new transaction",
		    EXPIRE_SNAPSHOTS_NAME, table_name.ToString());
	}
	return transaction_state->GetInfo();
}

void ValidateGCEnabled(const IcebergTableMetadata &metadata) {
	auto &properties = metadata.GetTableProperties();
	auto entry = properties.find("gc.enabled");
	if (entry == properties.end() || StringUtil::CIEquals(entry->second, "true")) {
		return;
	}
	if (StringUtil::CIEquals(entry->second, "false")) {
		throw InvalidInputException("%s: cannot run when table property 'gc.enabled' is false", EXPIRE_SNAPSHOTS_NAME);
	}
	throw InvalidInputException("%s: invalid value '%s' for table property 'gc.enabled': expected 'true' or 'false'",
	                            EXPIRE_SNAPSHOTS_NAME, entry->second);
}

void ApplyExpirationToTransactionMetadata(IcebergTableMetadata &metadata, const vector<int64_t> &snapshot_ids) {
	for (auto snapshot_id : snapshot_ids) {
		metadata.snapshots.erase(snapshot_id);
	}

	//! Remove the full prefix through the newest expired log entry. Keeping an
	//! older entry would let timestamp lookup cross a gap and return the wrong state.
	idx_t retained_log_start = 0;
	for (idx_t i = 0; i < metadata.snapshot_log.size(); i++) {
		if (!metadata.snapshots.count(metadata.snapshot_log[i].first)) {
			retained_log_start = i + 1;
		}
	}
	metadata.snapshot_log.erase(metadata.snapshot_log.begin(), metadata.snapshot_log.begin() + retained_log_start);
}

ExpireSnapshotsPlan ExecuteSnapshotExpiration(ClientContext &context, const ExpireSnapshotsPlanInput &input) {
	auto &catalog = GetMaintenanceIcebergCatalog(context, input.table_name, EXPIRE_SNAPSHOTS_NAME);
	auto &transaction = IcebergTransaction::Get(context, catalog);
	//! Planning and staging must use the same transaction-visible table state.
	auto &visible_table = LoadTransactionVisibleTable(context, input.table_name, catalog, transaction);
	if (visible_table.transaction_data) {
		visible_table.transaction_data->VerifySnapshotExpirationAllowed();
	}
	auto now = timestamp_ms_t(Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp()));
	auto plan = PlanSnapshotExpiration(visible_table.table_metadata, input, now);
	ValidateGCEnabled(visible_table.table_metadata);

	if (plan.to_expire.empty()) {
		return plan;
	}

	ApplyTableUpdate(visible_table, transaction, [&](IcebergTable &table) {
		auto &transaction_data = table.GetOrCreateTransactionData(transaction);
		transaction_data.TableRemoveSnapshots(plan.to_expire);
		//! Mirror the staged update so another call in this transaction replans
		//! from the remaining snapshots.
		ApplyExpirationToTransactionMetadata(table.table_metadata, plan.to_expire);
	});
	return plan;
}

struct ExpireSnapshotsGlobalSourceState : public GlobalSourceState {
	idx_t MaxThreads() override {
		return 1;
	}

	bool emitted = false;
};

} // namespace

LogicalExpireSnapshots::LogicalExpireSnapshots(idx_t bind_index_p, ExpireSnapshotsPlanInput input_p)
    : LogicalExtensionOperator(), bind_index(bind_index_p), input(std::move(input_p)) {
	SetEstimatedCardinality(1);
}

void LogicalExpireSnapshots::ResolveTypes() {
	types = ExpireSnapshotsResultTypes();
}

vector<ColumnBinding> LogicalExpireSnapshots::GetColumnBindings() {
	return GenerateColumnBindings(TableIndex(bind_index), 2);
}

vector<TableIndex> LogicalExpireSnapshots::GetTableIndex() const {
	return {TableIndex(bind_index)};
}

string LogicalExpireSnapshots::GetName() const {
	return "ICEBERG_EXPIRE_SNAPSHOTS";
}

PhysicalOperator &LogicalExpireSnapshots::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	return planner.Make<PhysicalExpireSnapshots>(std::move(input), estimated_cardinality);
}

PhysicalExpireSnapshots::PhysicalExpireSnapshots(PhysicalPlan &physical_plan, ExpireSnapshotsPlanInput input_p,
                                                 idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, ExpireSnapshotsResultTypes(),
                       estimated_cardinality),
      input(std::move(input_p)) {
}

unique_ptr<GlobalSourceState> PhysicalExpireSnapshots::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<ExpireSnapshotsGlobalSourceState>();
}

SourceResultType PhysicalExpireSnapshots::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                          OperatorSourceInput &source_input) const {
	auto &state = source_input.global_state.Cast<ExpireSnapshotsGlobalSourceState>();
	if (state.emitted) {
		return SourceResultType::FINISHED;
	}
	state.emitted = true;

	auto plan = ExecuteSnapshotExpiration(context.client, input);
	chunk.data[0].Append(Value::BIGINT(static_cast<int64_t>(plan.to_expire.size())));
	chunk.data[1].Append(Value::BIGINT(plan.retained_snapshots));
	return SourceResultType::FINISHED;
}

string PhysicalExpireSnapshots::GetName() const {
	return "ICEBERG_EXPIRE_SNAPSHOTS";
}

InsertionOrderPreservingMap<string> PhysicalExpireSnapshots::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = input.table_name.ToString();
	return result;
}

} // namespace duckdb
