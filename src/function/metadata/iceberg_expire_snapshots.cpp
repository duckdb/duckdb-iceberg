#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/binder.hpp"

#include "catalog/rest/iceberg_catalog.hpp"
#include "function/iceberg_functions.hpp"
#include "maintenance/expire_snapshots_operator.hpp"
#include "maintenance/expire_snapshots_planner.hpp"
#include "maintenance/maintenance_table_loader.hpp"

namespace duckdb {

namespace {

constexpr const char *EXPIRE_SNAPSHOTS_NAME = "iceberg_expire_snapshots";

QualifiedName ParseExpireSnapshotsTableName(const Value &identifier_value) {
	if (identifier_value.IsNull()) {
		throw InvalidInputException("%s: table identifier cannot be NULL", EXPIRE_SNAPSHOTS_NAME);
	}
	auto identifier = StringValue::Get(identifier_value);
	if (identifier.empty() || identifier.front() == '.' || identifier.back() == '.') {
		throw InvalidInputException("%s: table identifier '%s' has an empty component", EXPIRE_SNAPSHOTS_NAME,
		                            identifier);
	}
	auto parts = QualifiedName::ParseComponents(identifier);
	if (parts.size() != 3) {
		throw InvalidInputException("%s: table identifier must be 'catalog.schema.table', got '%s'",
		                            EXPIRE_SNAPSHOTS_NAME, identifier);
	}
	for (const auto &part : parts) {
		if (part.empty()) {
			throw InvalidInputException("%s: table identifier '%s' has an empty component", EXPIRE_SNAPSHOTS_NAME,
			                            identifier);
		}
	}
	return QualifiedName {parts[0], parts[1], parts[2]};
}

ExpireSnapshotsPlanInput BindExpireSnapshotsArguments(TableFunctionBindInput &input) {
	ExpireSnapshotsPlanInput result;
	result.table_name = ParseExpireSnapshotsTableName(input.inputs[0]);

	for (const auto &entry : input.named_parameters) {
		auto option = StringUtil::Lower(entry.first.GetIdentifierName());
		auto &value = entry.second;
		if (value.IsNull()) {
			if (option == "older_than" || option == "retain_last") {
				continue;
			}
			throw InvalidInputException("%s: '%s' cannot be NULL", EXPIRE_SNAPSHOTS_NAME, option);
		}
		if (option == "older_than") {
			auto older_than = value.GetValue<timestamp_t>();
			if (!older_than.IsFinite()) {
				throw InvalidInputException("%s: 'older_than' must be a finite timestamp", EXPIRE_SNAPSHOTS_NAME);
			}
			result.older_than = older_than;
		} else if (option == "retain_last") {
			auto retain_last = value.GetValue<int64_t>();
			if (retain_last < 1) {
				throw InvalidInputException("%s: 'retain_last' must be >= 1, got %lld", EXPIRE_SNAPSHOTS_NAME,
				                            retain_last);
			}
			result.retain_last = retain_last;
		} else if (option == "snapshot_ids") {
			if (value.type().id() != LogicalTypeId::LIST) {
				throw InvalidInputException("%s: 'snapshot_ids' must be a list of integers, got %s",
				                            EXPIRE_SNAPSHOTS_NAME, value.type().ToString());
			}
			auto &children = ListValue::GetChildren(value);
			result.snapshot_ids.reserve(children.size());
			for (const auto &child : children) {
				if (child.IsNull()) {
					throw InvalidInputException("%s: 'snapshot_ids' entries cannot be NULL", EXPIRE_SNAPSHOTS_NAME);
				}
				if (!child.type().IsIntegral()) {
					throw InvalidInputException("%s: 'snapshot_ids' entries must be integers, got %s",
					                            EXPIRE_SNAPSHOTS_NAME, child.type().ToString());
				}
				result.snapshot_ids.push_back(child.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>());
			}
		}
	}
	return result;
}

unique_ptr<LogicalOperator> ExpireSnapshotsBindOperator(ClientContext &context, TableFunctionBindInput &input,
                                                        TableIndex bind_index, vector<Identifier> &return_names) {
	if (!input.binder) {
		throw InternalException("%s: bind_operator called without a binder", EXPIRE_SNAPSHOTS_NAME);
	}
	auto plan_input = BindExpireSnapshotsArguments(input);
	//! Prepared maintenance statements must resolve the catalog again before execution.
	input.binder->SetAlwaysRequireRebind();
	auto &catalog = GetMaintenanceIcebergCatalog(context, plan_input.table_name, EXPIRE_SNAPSHOTS_NAME);
	DatabaseModificationType modification;
	modification |= DatabaseModificationType::ALTER_TABLE;
	input.binder->GetStatementProperties().RegisterDBModify(catalog, context, modification);

	return_names = {"deleted_snapshots", "retained_snapshots"};
	return make_uniq<LogicalExpireSnapshots>(bind_index.index, std::move(plan_input));
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergExpireSnapshotsFunction() {
	TableFunctionSet function_set(EXPIRE_SNAPSHOTS_NAME);
	TableFunction function(EXPIRE_SNAPSHOTS_NAME, {LogicalType::VARCHAR}, nullptr);
	function.bind_operator = ExpireSnapshotsBindOperator;
	function.named_parameters["older_than"] = LogicalType::TIMESTAMP;
	function.named_parameters["retain_last"] = LogicalType::BIGINT;
	function.named_parameters["snapshot_ids"] = LogicalType::ANY;
	function_set.AddFunction(function);
	return function_set;
}

} // namespace duckdb
