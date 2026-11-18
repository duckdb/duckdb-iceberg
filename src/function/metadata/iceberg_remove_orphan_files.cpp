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

#include <algorithm>

namespace duckdb {

namespace {

constexpr const char *REMOVE_ORPHAN_FILES_NAME = "iceberg_remove_orphan_files";

QualifiedName ParseTableIdentifier(const string &identifier) {
	if (identifier.empty() || identifier.front() == '.' || identifier.back() == '.' ||
	    identifier.find("..") != string::npos) {
		throw InvalidInputException("%s: table identifier has an empty component (got '%s')", REMOVE_ORPHAN_FILES_NAME,
		                            identifier);
	}
	auto parts = QualifiedName::ParseComponents(identifier);
	if (parts.size() != 3) {
		throw InvalidInputException("%s: table identifier must be 'catalog.schema.table', got '%s'",
		                            REMOVE_ORPHAN_FILES_NAME, identifier);
	}
	const bool quoted = identifier.find('"') != string::npos;
	for (const auto &part : parts) {
		const auto &name = part.GetIdentifierName();
		if (quoted || StringUtil::CharacterIsDigit(name.front()) || !std::all_of(name.begin(), name.end(), [](char c) {
			    return c == '_' || StringUtil::CharacterIsAlphaNumeric(c);
		    })) {
			throw InvalidInputException("%s requires a simple catalog.schema.table identifier; "
			                            "quoted identifiers and nested namespaces are not accepted (got '%s')",
			                            REMOVE_ORPHAN_FILES_NAME, identifier);
		}
	}
	return QualifiedName {parts[0], parts[1], parts[2]};
}

RemoveOrphanFilesPlanInput BindRemoveOrphanFilesArguments(TableFunctionBindInput &input) {
	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("%s: table identifier cannot be NULL", REMOVE_ORPHAN_FILES_NAME);
	}
	RemoveOrphanFilesPlanInput result;
	result.table_name = ParseTableIdentifier(StringValue::Get(input.inputs[0]));
	for (const auto &entry : input.named_parameters) {
		auto option = StringUtil::Lower(entry.first.GetIdentifierName());
		auto &value = entry.second;
		if (value.IsNull()) {
			throw InvalidInputException("%s: '%s' cannot be NULL", REMOVE_ORPHAN_FILES_NAME, option);
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
	auto plan_input = BindRemoveOrphanFilesArguments(input);
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
	return make_uniq<LogicalRemoveOrphanFiles>(bind_index.index, std::move(plan_input));
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergRemoveOrphanFilesFunction() {
	TableFunctionSet function_set(REMOVE_ORPHAN_FILES_NAME);
	TableFunction function(REMOVE_ORPHAN_FILES_NAME,
	                       FunctionSignature().AddPositionalOnly("table_name", LogicalType::VARCHAR), nullptr);
	function.bind_operator = RemoveOrphanFilesBindOperator;
	function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("older_than", LogicalType::TIMESTAMP)
		    .Add("location", LogicalType::VARCHAR)
		    .Add("dry_run", LogicalType::BOOLEAN);
	});
	function_set.AddFunction(function);
	return function_set;
}

} // namespace duckdb
