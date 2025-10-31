#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/file_system.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_functions.hpp"
#include "iceberg_utils.hpp"
#include "../include/storage/irc_table_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/iceberg_table_information.hpp"
#include "storage/iceberg_transaction_data.hpp"
#include "storage/irc_table_entry.hpp"
#include "storage/irc_transaction.hpp"
#include "storage/iceberg_table_information.hpp"
#include "metadata/iceberg_table_metadata.hpp"

#include <string>

namespace duckdb {

struct SetIcebergTablePropertiesBindData : public TableFunctionData {
	optional_ptr<ICTableEntry> iceberg_table;
	case_insensitive_map_t<string> properties;
	vector<string> remove_properties;
};

struct SetIcebergTablePropertiesGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	SetIcebergTablePropertiesGlobalTableFunctionState() {};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<SetIcebergTablePropertiesGlobalTableFunctionState>();
	}

	idx_t property_count = 0;
	// FIXME: this is probably super dumb, but since properties are just string->string, we will keep them in
	// memory like this for now.
	// need to keep the properties ordered when returning them in case there are 2048+
	// that way we don't duplicate certain properties
	vector<pair<string, string>> all_properties;
	bool all_properties_initialized = false;
	bool properties_set = false;
	bool properties_removed = false;
};

static optional_ptr<ICTableEntry> GetTableEntry(ClientContext &context, string &input_string) {

	auto qualified_name = QualifiedName::ParseComponents(input_string);
	optional_ptr<ICTableEntry> iceberg_table = nullptr;
	auto default_db = DatabaseManager::GetDefaultDatabase(context);
	auto &catalog = Catalog::GetCatalog(context, default_db);
	switch (qualified_name.size()) {
	case 3: {
		auto lookup_info = EntryLookupInfo(CatalogType::TABLE_ENTRY, qualified_name[2]);
		auto table =
		    Catalog::GetEntry(context, qualified_name[0], qualified_name[1], lookup_info, OnEntryNotFound::RETURN_NULL);
		if (!table) {
			return nullptr;
		}
		auto &table_catalog = table->ParentSchema().ParentCatalog();
		if (table_catalog.GetCatalogType() != "iceberg") {
			throw InvalidInputException("Cannot call iceberg_table_properties() on non-iceberg Table");
		}
		iceberg_table = table->Cast<ICTableEntry>();
		break;
	}
	case 2: {
		// assume "schema"."table"
		if (catalog.GetCatalogType() != "iceberg") {
			throw InvalidInputException("Cannot call iceberg_table_properties() on non-iceberg Table");
		}
		// make sure default catalog is iceberg
		auto table_entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, qualified_name[0], qualified_name[1],
		                                    OnEntryNotFound::RETURN_NULL);
		if (!table_entry) {
			return nullptr;
		}
		iceberg_table = table_entry->Cast<ICTableEntry>();
		break;
	}
	case 1: {
		// assume "table"
		if (catalog.GetCatalogType() != "iceberg") {
			auto &fs = FileSystem::GetFileSystem(context);
			if (fs.DirectoryExists(input_string) || fs.FileExists(input_string)) {
				throw InvalidInputException("Cannot call iceberg_table_properties() on a file/directory");
			}
			throw InvalidInputException("Cannot call iceberg_table_properties() on non-iceberg Table");
		}
		auto schema = catalog.GetDefaultSchema();
		auto table_entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, schema, qualified_name[0],
		                                    OnEntryNotFound::RETURN_NULL);
		if (!table_entry) {
			return nullptr;
		}
		iceberg_table = table_entry->Cast<ICTableEntry>();
		break;
	}
	default:
		throw InvalidInputException("Cannot have four identifiers in table name %s", input_string);
	}
	return iceberg_table;
}

static unique_ptr<FunctionData> SetIcebergTablePropertiesBind(ClientContext &context, TableFunctionBindInput &input,
                                                              vector<LogicalType> &return_types,
                                                              vector<string> &names) {
	// return a TableRef that contains the scans for the
	auto ret = make_uniq<SetIcebergTablePropertiesBindData>();

	auto input_string = input.inputs[0].ToString();
	auto iceberg_table = GetTableEntry(context, input_string);
	ret->iceberg_table = iceberg_table;
	auto map = Value(input.inputs[1]).DefaultCastAs(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));

	auto &map_children = MapValue::GetChildren(map);
	for (idx_t col_idx = 0; col_idx < map_children.size(); col_idx++) {
		auto &struct_children = StructValue::GetChildren(map_children[col_idx]);
		auto &key = StringValue::Get(struct_children[0]);
		auto &val = StringValue::Get(struct_children[1]);
		ret->properties.emplace(key, val);
	}

	return_types.insert(return_types.end(), LogicalType::BIGINT);
	names.insert(names.end(), string("Success"));
	return std::move(ret);
}

static unique_ptr<FunctionData> RemoveIcebergTablePropertiesBind(ClientContext &context, TableFunctionBindInput &input,
                                                                 vector<LogicalType> &return_types,
                                                                 vector<string> &names) {
	// return a TableRef that contains the scans for the
	auto ret = make_uniq<SetIcebergTablePropertiesBindData>();

	auto input_string = input.inputs[0].ToString();
	auto iceberg_table = GetTableEntry(context, input_string);
	ret->iceberg_table = iceberg_table;

	auto &remove_values = input.inputs[1];
	auto &list_children = ListValue::GetChildren(remove_values);
	for (idx_t col_idx = 0; col_idx < list_children.size(); col_idx++) {
		auto &remove_property = StringValue::Get(list_children[0]);
		ret->remove_properties.push_back(remove_property);
	}

	return_types.insert(return_types.end(), LogicalType::BIGINT);
	names.insert(names.end(), string("Success"));
	return std::move(ret);
}

static unique_ptr<FunctionData> GetIcebergTablePropertiesBind(ClientContext &context, TableFunctionBindInput &input,
                                                              vector<LogicalType> &return_types,
                                                              vector<string> &names) {
	// return a TableRef that contains the scans for the
	auto ret = make_uniq<SetIcebergTablePropertiesBindData>();
	auto input_string = input.inputs[0].ToString();
	auto iceberg_table = GetTableEntry(context, input_string);

	D_ASSERT(iceberg_table);
	ret->iceberg_table = iceberg_table;

	return_types.insert(return_types.end(), LogicalType::VARCHAR);
	return_types.insert(return_types.end(), LogicalType::VARCHAR);
	names.insert(names.end(), string("key"));
	names.insert(names.end(), string("value"));
	return std::move(ret);
}

static void AddString(Vector &vec, idx_t index, string_t &&str) {
	FlatVector::GetData<string_t>(vec)[index] = StringVector::AddString(vec, std::move(str));
}

static void SetIcebergTablePropertiesFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<SetIcebergTablePropertiesBindData>();
	auto &global_state = data.global_state->Cast<SetIcebergTablePropertiesGlobalTableFunctionState>();

	if (!bind_data.iceberg_table) {
		//! Table is empty
		return;
	}
	if (global_state.properties_set) {
		output.SetCardinality(0);
		return;
	}

	auto iceberg_table = bind_data.iceberg_table;
	auto &irc_transaction = IRCTransaction::Get(context, iceberg_table->catalog);
	if (!iceberg_table->table_info.transaction_data) {
		iceberg_table->table_info.transaction_data =
		    make_uniq<IcebergTransactionData>(context, iceberg_table->table_info);
	}
	IcebergTransactionData &transaction_data = *(iceberg_table->table_info.transaction_data);

	auto schema = iceberg_table->schema.name;
	auto table_name = iceberg_table->name;
	transaction_data.TableSetProperties(bind_data.properties);
	irc_transaction.dirty_tables.emplace(iceberg_table.get());
	global_state.properties_set = true;
	// set success output, failure happens during transaction commit.
	FlatVector::GetData<int64_t>(output.data[0])[0] = bind_data.properties.size();
	output.SetCardinality(1);
}

static void RemoveIcebergTablePropertiesFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<SetIcebergTablePropertiesBindData>();
	auto &global_state = data.global_state->Cast<SetIcebergTablePropertiesGlobalTableFunctionState>();

	if (!bind_data.iceberg_table) {
		//! Table is empty
		return;
	}
	if (global_state.properties_removed) {
		output.SetCardinality(0);
		return;
	}

	auto iceberg_table = bind_data.iceberg_table;
	auto &irc_transaction = IRCTransaction::Get(context, iceberg_table->catalog);
	if (!iceberg_table->table_info.transaction_data) {
		iceberg_table->table_info.transaction_data =
		    make_uniq<IcebergTransactionData>(context, iceberg_table->table_info);
	}
	IcebergTransactionData &transaction_data = *(iceberg_table->table_info.transaction_data);

	auto schema = iceberg_table->schema.name;
	auto table_name = iceberg_table->name;
	transaction_data.TableRemoveProperties(bind_data.remove_properties);
	irc_transaction.dirty_tables.emplace(iceberg_table.get());
	global_state.properties_removed = true;
	// set success output, failure happens during transaction commit.
	FlatVector::GetData<int64_t>(output.data[0])[0] = bind_data.properties.size();
	output.SetCardinality(1);
}

static void GetIcebergTablePropertiesFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<SetIcebergTablePropertiesBindData>();
	auto &global_state = data.global_state->Cast<SetIcebergTablePropertiesGlobalTableFunctionState>();

	if (!bind_data.iceberg_table) {
		//! Table is empty
		return;
	}
	auto iceberg_table = bind_data.iceberg_table;
	auto &table_info = iceberg_table->table_info;
	if (!table_info.load_table_result.metadata.has_properties) {
		output.SetCardinality(0);
		return;
	}
	if (!global_state.all_properties_initialized) {
		for (auto &property : table_info.load_table_result.metadata.properties) {
			global_state.all_properties.push_back(make_pair(property.first, property.second));
		}
		global_state.all_properties_initialized = true;
	}
	// if we have already returned all properties.
	if (global_state.property_count >= global_state.all_properties.size()) {
		output.SetCardinality(0);
		return;
	}

	idx_t row_number = 0;
	for (idx_t prop_index = global_state.property_count; prop_index < global_state.all_properties.size();
	     ++prop_index) {
		auto &property = global_state.all_properties[prop_index];
		AddString(output.data[0], row_number, string_t(property.first));
		AddString(output.data[1], row_number, string_t(property.second));
		row_number++;
		if (row_number >= STANDARD_VECTOR_SIZE) {
			break;
		}
	}
	global_state.property_count += row_number;
	output.SetCardinality(row_number);
}

TableFunctionSet IcebergFunctions::SetIcebergTablePropertiesFunctions() {
	TableFunctionSet function_set("set_iceberg_table_properties");

	auto fun = TableFunction({LogicalType::VARCHAR, LogicalType::ANY}, SetIcebergTablePropertiesFunction,
	                         SetIcebergTablePropertiesBind, SetIcebergTablePropertiesGlobalTableFunctionState::Init);
	function_set.AddFunction(fun);

	return function_set;
}

TableFunctionSet IcebergFunctions::RemoveIcebergTablePropertiesFunctions() {
	TableFunctionSet function_set("remove_iceberg_table_properties");

	auto fun = TableFunction({LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)},
	                         RemoveIcebergTablePropertiesFunction, RemoveIcebergTablePropertiesBind,
	                         SetIcebergTablePropertiesGlobalTableFunctionState::Init);
	function_set.AddFunction(fun);

	return function_set;
}

TableFunctionSet IcebergFunctions::GetIcebergTablePropertiesFunctions() {
	TableFunctionSet function_set("iceberg_table_properties");

	auto fun = TableFunction({LogicalType::VARCHAR}, GetIcebergTablePropertiesFunction, GetIcebergTablePropertiesBind,
	                         SetIcebergTablePropertiesGlobalTableFunctionState::Init);
	function_set.AddFunction(fun);

	return function_set;
}

} // namespace duckdb
