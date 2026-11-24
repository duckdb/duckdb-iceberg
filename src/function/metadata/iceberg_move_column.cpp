#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/binder.hpp"

#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "function/iceberg_functions.hpp"

namespace duckdb {

namespace {

struct IcebergMoveColumnBindData : public TableFunctionData {
	optional_ptr<IcebergTableSchemaVersion> iceberg_table;
	vector<Identifier> column_path;
	IcebergColumnMove move = IcebergColumnMove::FIRST;
	vector<Identifier> reference_path;
};

struct IcebergMoveColumnGlobalState : public GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &, TableFunctionInitInput &) {
		return make_uniq<IcebergMoveColumnGlobalState>();
	}

	bool finished = false;
};

//! A column path as ALTER TABLE takes it: 'c', 's.f', 'l.element.f', with double quotes around a name that holds a dot
static vector<Identifier> ParseColumnPath(const Value &value, const string &argument) {
	if (value.IsNull()) {
		throw InvalidInputException("iceberg_move_column: %s cannot be NULL", argument);
	}
	auto input = value.ToString();
	QualifiedName path;
	string error;
	//! TryParse drops an empty last component, so 's.' is refused here
	if (StringUtil::EndsWith(input, ".") || !QualifiedName::TryParse(input, path, error)) {
		throw InvalidInputException("iceberg_move_column: %s '%s' is not a column name", argument, input);
	}
	return path.Path();
}

static unique_ptr<FunctionData> IcebergMoveColumnBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto ret = make_uniq<IcebergMoveColumnBindData>();

	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("iceberg_move_column: table_name cannot be NULL");
	}
	ret->column_path = ParseColumnPath(input.inputs[1], "column_name");
	for (auto &entry : input.named_parameters) {
		if (entry.first == "first") {
			if (entry.second.IsNull()) {
				throw InvalidInputException("iceberg_move_column: 'first' cannot be NULL");
			}
			if (!BooleanValue::Get(entry.second)) {
				throw InvalidInputException("iceberg_move_column: 'first' cannot be false");
			}
			ret->move = IcebergColumnMove::FIRST;
		} else if (entry.first == "before") {
			ret->move = IcebergColumnMove::BEFORE;
			ret->reference_path = ParseColumnPath(entry.second, "'before'");
		} else if (entry.first == "after") {
			ret->move = IcebergColumnMove::AFTER;
			ret->reference_path = ParseColumnPath(entry.second, "'after'");
		} else {
			throw InternalException("iceberg_move_column: unhandled named parameter '%s'",
			                        entry.first.GetIdentifierName());
		}
	}
	if (input.named_parameters.size() != 1) {
		throw InvalidInputException("iceberg_move_column: exactly one of 'first', 'after' and 'before' must be given");
	}

	auto table_name = input.inputs[0].ToString();
	auto table_entry = IcebergUtils::GetTableEntry(context, table_name);
	if (!IcebergUtils::IsIcebergCatalog(table_entry->ParentCatalog()) ||
	    table_entry->type != CatalogType::TABLE_ENTRY) {
		throw InvalidInputException("Cannot call iceberg_move_column on non-iceberg table");
	}
	ret->iceberg_table = table_entry->Cast<IcebergTableSchemaVersion>();

	if (input.binder) {
		//! The table entry belongs to the transaction that binds it: a prepared statement resolves it again
		input.binder->SetAlwaysRequireRebind();
		DatabaseModificationType modification;
		modification |= DatabaseModificationType::ALTER_TABLE;
		input.binder->GetStatementProperties().RegisterDBModify(table_entry->ParentCatalog(), context, modification);
	}

	return_types.push_back(LogicalType::INTEGER);
	return_types.push_back(LogicalType::INTEGER);
	names.emplace_back("previous_schema_id");
	names.emplace_back("current_schema_id");
	return std::move(ret);
}

static void IcebergMoveColumnFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergMoveColumnBindData>();
	auto &global_state = data.global_state->Cast<IcebergMoveColumnGlobalState>();
	if (global_state.finished) {
		output.SetChildCardinality(0);
		return;
	}

	auto iceberg_table = bind_data.iceberg_table;
	auto &table = *iceberg_table;
	auto &schema = table.ParentSchema().Cast<IcebergSchemaEntry>();
	auto schema_ids =
	    schema.MoveColumn(context, table, bind_data.column_path, bind_data.move, bind_data.reference_path);

	FlatVector::GetDataMutable<int32_t>(output.data[0])[0] = schema_ids.first;
	FlatVector::GetDataMutable<int32_t>(output.data[1])[0] = schema_ids.second;
	output.SetChildCardinality(1);
	global_state.finished = true;
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergMoveColumnFunction() {
	TableFunctionSet function_set("iceberg_move_column");
	TableFunction function(FunctionSignature()
	                           .AddPositionalOnly("table_name", LogicalType::VARCHAR)
	                           .AddPositionalOnly("column_name", LogicalType::VARCHAR),
	                       IcebergMoveColumnFunction, IcebergMoveColumnBind, IcebergMoveColumnGlobalState::Init);
	function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("first", LogicalType::BOOLEAN)
		    .Add("after", LogicalType::VARCHAR)
		    .Add("before", LogicalType::VARCHAR);
	});
	function_set.AddFunction(function);
	return function_set;
}

} // namespace duckdb
