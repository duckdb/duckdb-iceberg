#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"

#include "function/iceberg_functions.hpp"
#include "common/iceberg_utils.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

#include <string>

namespace duckdb {

struct IcebergSchemaPropertiesBindData : public TableFunctionData {
	optional_ptr<IcebergSchemaEntry> iceberg_schema;
	case_insensitive_map_t<string> properties;
	set<string> remove_properties;
};

struct IcebergSchemaPropertiesGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	IcebergSchemaPropertiesGlobalTableFunctionState() {};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IcebergSchemaPropertiesGlobalTableFunctionState>();
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

static bool CheckEntryIsIcebergSchema(optional_ptr<SchemaCatalogEntry> entry) {
	auto &catalog_entry = entry->Cast<InCatalogEntry>();
	auto &catalog = catalog_entry.catalog;
	if (catalog.GetCatalogType() != "iceberg") {
		return false;
	}
	if (entry->type != CatalogType::SCHEMA_ENTRY) {
		return false;
	}
	return true;
}

static unique_ptr<FunctionData> SetIcebergSchemaPropertiesBind(ClientContext &context, TableFunctionBindInput &input,
                                                               vector<LogicalType> &return_types,
                                                               vector<string> &names) {
	auto ret = make_uniq<IcebergSchemaPropertiesBindData>();

	auto input_string = input.inputs[0].ToString();
	auto iceberg_schema = IcebergUtils::GetSchemaEntry(context, input_string);
	if (!CheckEntryIsIcebergSchema(iceberg_schema)) {
		throw InvalidInputException("Cannot call set_iceberg_schema_properties on non-iceberg schema");
	}
	ret->iceberg_schema = iceberg_schema->Cast<IcebergSchemaEntry>();
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

static unique_ptr<FunctionData> RemoveIcebergSchemaPropertiesBind(ClientContext &context, TableFunctionBindInput &input,
                                                                  vector<LogicalType> &return_types,
                                                                  vector<string> &names) {
	auto ret = make_uniq<IcebergSchemaPropertiesBindData>();
	auto input_string = input.inputs[0].ToString();
	auto iceberg_schema = IcebergUtils::GetSchemaEntry(context, input_string);
	if (!CheckEntryIsIcebergSchema(iceberg_schema)) {
		throw InvalidInputException("Cannot call remove_iceberg_schema_properties on non-iceberg schema");
	}
	ret->iceberg_schema = iceberg_schema->Cast<IcebergSchemaEntry>();

	auto &remove_values = input.inputs[1];
	auto &list_children = ListValue::GetChildren(remove_values);
	for (idx_t col_idx = 0; col_idx < list_children.size(); col_idx++) {
		auto &remove_property = StringValue::Get(list_children[0]);
		ret->remove_properties.insert(remove_property);
	}

	return_types.insert(return_types.end(), LogicalType::BIGINT);
	names.insert(names.end(), string("Success"));
	return std::move(ret);
}

static unique_ptr<FunctionData> GetIcebergSchemaPropertiesBind(ClientContext &context, TableFunctionBindInput &input,
                                                               vector<LogicalType> &return_types,
                                                               vector<string> &names) {
	auto ret = make_uniq<IcebergSchemaPropertiesBindData>();
	auto input_string = input.inputs[0].ToString();
	auto iceberg_schema = IcebergUtils::GetSchemaEntry(context, input_string);
	if (!CheckEntryIsIcebergSchema(iceberg_schema)) {
		throw InvalidInputException("Cannot call get_iceberg_schema_properties on non-iceberg schema");
	}
	ret->iceberg_schema = iceberg_schema->Cast<IcebergSchemaEntry>();

	return_types.insert(return_types.end(), LogicalType::VARCHAR);
	return_types.insert(return_types.end(), LogicalType::VARCHAR);
	names.insert(names.end(), string("key"));
	names.insert(names.end(), string("value"));
	return std::move(ret);
}

static void AddString(Vector &vec, idx_t index, string_t &&str) {
	FlatVector::GetData<string_t>(vec)[index] = StringVector::AddString(vec, std::move(str));
}

static void SetIcebergSchemaPropertiesFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergSchemaPropertiesBindData>();
	auto &global_state = data.global_state->Cast<IcebergSchemaPropertiesGlobalTableFunctionState>();

	if (!bind_data.iceberg_schema) {
		return;
	}
	if (global_state.properties_set) {
		output.SetCardinality(0);
		return;
	}

	auto iceberg_schema = bind_data.iceberg_schema;
	iceberg_schema->LoadProperties(context);

	// reflect changes to IcebergSchemaEntry
	for (auto property : bind_data.properties) {
		iceberg_schema->schema_info.properties[property.first] = property.second;
	}

	// populate removals to be sent to IRC API
	auto &iceberg_transaction = IcebergTransaction::Get(context, iceberg_schema->catalog);
	auto schema_key = iceberg_schema->GetSchemaKey();
	if (iceberg_transaction.schema_property_updates.find(schema_key) ==
	    iceberg_transaction.schema_property_updates.end()) {
		// not present, create one
		auto properties = SchemaPropertyUpdates();
		properties.updates = bind_data.properties;
		iceberg_transaction.schema_property_updates[schema_key] = properties;
	} else {
		auto &schema_property_updates = iceberg_transaction.schema_property_updates[schema_key];
		auto &removals = schema_property_updates.removals;
		auto &updates = schema_property_updates.updates;
		for (auto &property : bind_data.properties) {
			if (removals.find(property.first) != removals.end()) {
				removals.erase(property.first);
			}
			updates[property.first] = property.second;
		}
	}

	global_state.properties_set = true;
	// set success output, failure happens during transaction commit.
	FlatVector::GetData<int64_t>(output.data[0])[0] = iceberg_schema->schema_info.properties.size();
	output.SetCardinality(1);
}

static void RemoveIcebergSchemaPropertiesFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergSchemaPropertiesBindData>();
	auto &global_state = data.global_state->Cast<IcebergSchemaPropertiesGlobalTableFunctionState>();

	if (!bind_data.iceberg_schema) {
		return;
	}
	if (global_state.properties_removed) {
		output.SetCardinality(0);
		return;
	}

	auto iceberg_schema = bind_data.iceberg_schema;
	iceberg_schema->LoadProperties(context);

	auto schema_key = iceberg_schema->GetSchemaKey();

	// reflect changes to IcebergSchemaEntry
	for (auto property_to_remove : bind_data.remove_properties) {
		iceberg_schema->schema_info.properties.erase(property_to_remove);
	}

	// populate removals to be sent to IRC API
	auto &iceberg_transaction = IcebergTransaction::Get(context, iceberg_schema->catalog);
	if (iceberg_transaction.schema_property_updates.find(schema_key) ==
	    iceberg_transaction.schema_property_updates.end()) {
		// not present, create one
		auto properties = SchemaPropertyUpdates();
		properties.removals = bind_data.remove_properties;
		iceberg_transaction.schema_property_updates[schema_key] = properties;

	} else {
		auto &schema_property_updates = iceberg_transaction.schema_property_updates[schema_key];
		auto &removals = schema_property_updates.removals;
		auto &updates = schema_property_updates.updates;

		for (auto &property_to_remove : bind_data.remove_properties) {
			if (updates.find(property_to_remove) != updates.end()) {
				updates.erase(property_to_remove);
			}
			removals.insert(property_to_remove);
		}
	}

	global_state.properties_removed = true;
	// set success output, failure happens during transaction commit.
	FlatVector::GetData<int64_t>(output.data[0])[0] = iceberg_schema->schema_info.properties.size();
	output.SetCardinality(1);
}

static void GetIcebergSchemaPropertiesFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergSchemaPropertiesBindData>();
	auto &global_state = data.global_state->Cast<IcebergSchemaPropertiesGlobalTableFunctionState>();

	if (!bind_data.iceberg_schema) {
		return;
	}
	auto iceberg_schema = bind_data.iceberg_schema;
	iceberg_schema->LoadProperties(context);

	auto schema_key = iceberg_schema->GetSchemaKey();

	auto &iceberg_transaction = IcebergTransaction::Get(context, iceberg_schema->catalog);
	auto &schema_properties = iceberg_schema->schema_info.properties;

	if (schema_properties.empty()) {
		output.SetCardinality(0);
		return;
	}
	if (!global_state.all_properties_initialized) {
		for (auto &property : schema_properties) {
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

TableFunctionSet IcebergFunctions::SetIcebergSchemaPropertiesFunctions() {
	TableFunctionSet function_set("set_iceberg_schema_properties");

	auto fun = TableFunction({LogicalType::VARCHAR, LogicalType::ANY}, SetIcebergSchemaPropertiesFunction,
	                         SetIcebergSchemaPropertiesBind, IcebergSchemaPropertiesGlobalTableFunctionState::Init);
	function_set.AddFunction(fun);

	return function_set;
}

TableFunctionSet IcebergFunctions::RemoveIcebergSchemaPropertiesFunctions() {
	TableFunctionSet function_set("remove_iceberg_schema_properties");

	auto fun = TableFunction({LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)},
	                         RemoveIcebergSchemaPropertiesFunction, RemoveIcebergSchemaPropertiesBind,
	                         IcebergSchemaPropertiesGlobalTableFunctionState::Init);
	function_set.AddFunction(fun);

	return function_set;
}

TableFunctionSet IcebergFunctions::GetIcebergSchemaPropertiesFunctions() {
	TableFunctionSet function_set("iceberg_schema_properties");

	auto fun = TableFunction({LogicalType::VARCHAR}, GetIcebergSchemaPropertiesFunction, GetIcebergSchemaPropertiesBind,
	                         IcebergSchemaPropertiesGlobalTableFunctionState::Init);
	function_set.AddFunction(fun);

	return function_set;
}

} // namespace duckdb
