#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"

#include "catalog/rest/iceberg_catalog.hpp"
#include "function/iceberg_functions.hpp"
#include "maintenance/maintenance_table_loader.hpp"
#include "maintenance/remove_orphan_files_operator.hpp"

namespace duckdb {

namespace {

constexpr const char *REMOVE_ORPHAN_FILES_NAME = "iceberg_remove_orphan_files";

RemoveOrphanFilesPlanInput BindRemoveOrphanFilesArguments(ClientContext &context, TableFunctionBindInput &input) {
	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("%s: table identifier cannot be NULL", REMOVE_ORPHAN_FILES_NAME);
	}
	const auto &identifier = StringValue::Get(input.inputs[0]);
	if (!identifier.empty() && identifier.back() == '.') {
		throw InvalidInputException("Table name %s has an empty component", identifier);
	}
	RemoveOrphanFilesPlanInput result;
	result.table_name = ResolveMaintenanceTableName(context, identifier, REMOVE_ORPHAN_FILES_NAME);
	for (const auto &entry : input.named_parameters) {
		const auto &option = entry.first;
		auto &value = entry.second;
		if (value.IsNull()) {
			throw InvalidInputException("%s: '%s' cannot be NULL", REMOVE_ORPHAN_FILES_NAME,
			                            StringUtil::Lower(option.GetIdentifierName()));
		}
		if (option == "older_than") {
			auto older_than = value.GetValue<timestamp_t>();
			if (!older_than.IsFinite()) {
				throw InvalidInputException("%s: 'older_than' must be a finite timestamp", REMOVE_ORPHAN_FILES_NAME);
			}
			result.older_than = older_than;
		} else if (option == "location") {
			result.location = StringValue::Get(value);
			if (result.location.empty()) {
				throw InvalidInputException("%s: 'location' cannot be empty", REMOVE_ORPHAN_FILES_NAME);
			}
		} else if (option == "dry_run") {
			result.dry_run = BooleanValue::Get(value);
		}
	}
	return result;
}

unique_ptr<LogicalOperator> RemoveOrphanFilesBindOperator(ClientContext &context, TableFunctionBindInput &input,
                                                          TableIndex bind_index, vector<Identifier> &return_names) {
	if (input.ref.with_ordinality == OrdinalityType::WITH_ORDINALITY) {
		throw NotImplementedException("%s does not support WITH ORDINALITY", REMOVE_ORPHAN_FILES_NAME);
	}
	auto plan_input = BindRemoveOrphanFilesArguments(context, input);
	if (!plan_input.dry_run) {
		input.binder->SetAlwaysRequireRebind();
		auto &catalog = GetMaintenanceIcebergCatalog(context, plan_input.table_name, REMOVE_ORPHAN_FILES_NAME);
		input.binder->GetStatementProperties().RegisterDBModify(catalog, context,
		                                                        DatabaseModificationType::DELETE_DATA);
	}
	return_names = {"orphan_file_locations", "deleted_file_count", "scanned_files", "referenced_files", "error_code",
	                "failed_file_locations", "failed_file_count"};
	const auto &column_name_alias = input.ref.column_name_alias;
	for (idx_t i = 0; i < column_name_alias.size() && i < return_names.size(); i++) {
		return_names[i] = column_name_alias[i];
	}
	return make_uniq<LogicalRemoveOrphanFiles>(bind_index, std::move(plan_input));
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergRemoveOrphanFilesFunction() {
	TableFunction function(REMOVE_ORPHAN_FILES_NAME,
	                       FunctionSignature().AddPositionalOnly("table_name", LogicalType::VARCHAR), nullptr);
	function.bind_operator = RemoveOrphanFilesBindOperator;
	function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("older_than", LogicalType::TIMESTAMP)
		    .Add("location", LogicalType::VARCHAR)
		    .Add("dry_run", LogicalType::BOOLEAN);
	});
	return TableFunctionSet(std::move(function));
}

} // namespace duckdb
