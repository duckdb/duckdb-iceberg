#include "storage/table_update/iceberg_add_snapshot.hpp"
#include "storage/table_create/iceberg_create_table_request.hpp"
#include "storage/irc_table_set.hpp"
#include "utils/iceberg_type.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/caching_file_system.hpp"

using namespace duckdb_yyjson;
namespace duckdb {

IcebergCreateTableRequest::IcebergCreateTableRequest(shared_ptr<IcebergTableSchema> schema, string table_name)
    : table_name(table_name), initial_schema(schema) {
}

static void AddUnnamedField(yyjson_mut_doc *doc, yyjson_mut_val *field_obj, IcebergColumnDefinition &column);

static void AddNamedField(yyjson_mut_doc *doc, yyjson_mut_val *field_obj, IcebergColumnDefinition &column) {
	yyjson_mut_obj_add_strcpy(doc, field_obj, "name", column.name.c_str());
	yyjson_mut_obj_add_uint(doc, field_obj, "id", column.id);
	if (column.type.IsNested()) {
		auto type_obj = yyjson_mut_obj_add_obj(doc, field_obj, "type");
		AddUnnamedField(doc, type_obj, column);
		yyjson_mut_obj_add_bool(doc, field_obj, "required", column.required);
		return;
	}
	yyjson_mut_obj_add_strcpy(doc, field_obj, "type", IcebergTypeHelper::LogicalTypeToIcebergType(column.type).c_str());
	yyjson_mut_obj_add_bool(doc, field_obj, "required", column.required);
}

static void AddUnnamedField(yyjson_mut_doc *doc, yyjson_mut_val *field_obj, IcebergColumnDefinition &column) {
	D_ASSERT(column.type.IsNested());
	switch (column.type.id()) {
	case LogicalTypeId::STRUCT: {
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type", "struct");
		auto nested_fields_arr = yyjson_mut_obj_add_arr(doc, field_obj, "fields");
		for (auto &field : column.children) {
			auto nested_field_obj = yyjson_mut_arr_add_obj(doc, nested_fields_arr);
			AddNamedField(doc, nested_field_obj, *field);
		}
		break;
	}
	case LogicalTypeId::LIST: {
		auto type_obj = yyjson_mut_obj_add_obj(doc, field_obj, "type");
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type", "list");
		D_ASSERT(column.children.size() == 1);
		auto &list_type = column.children[0];
		yyjson_mut_obj_add_uint(doc, field_obj, "element-id", list_type->id);
		if (list_type->IsPrimitiveType()) {
			yyjson_mut_obj_add_strcpy(doc, field_obj, "element",
			                          IcebergTypeHelper::LogicalTypeToIcebergType(list_type->type).c_str());
		} else {
			auto list_type_obj = yyjson_mut_obj_add_obj(doc, field_obj, "element");
			AddUnnamedField(doc, list_type_obj, *list_type);
		}
		yyjson_mut_obj_add_bool(doc, field_obj, "element-required", false);
		return;
	}
	case LogicalTypeId::MAP: {
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type", "map");
		D_ASSERT(column.children.size() == 2);
		auto &key_child = column.children[0];
		if (key_child->IsPrimitiveType()) {
			yyjson_mut_obj_add_strcpy(doc, field_obj, "key",
			                          IcebergTypeHelper::LogicalTypeToIcebergType(key_child->type).c_str());
		} else {
			auto key_obj = yyjson_mut_obj_add_obj(doc, field_obj, "key");
			yyjson_mut_obj_add_strcpy(doc, key_obj, "type",
			                          IcebergTypeHelper::LogicalTypeToIcebergType(key_child->type).c_str());
			auto nested_key_fields_arr = yyjson_mut_obj_add_arr(doc, key_obj, "fields");
			AddNamedField(doc, nested_key_fields_arr, *key_child);
		}
		yyjson_mut_obj_add_uint(doc, field_obj, "key-id", key_child->id);
		auto &val_child = column.children[1];
		if (val_child->IsPrimitiveType()) {
			yyjson_mut_obj_add_strcpy(doc, field_obj, "value",
			                          IcebergTypeHelper::LogicalTypeToIcebergType(val_child->type).c_str());
		} else {
			auto val_obj = yyjson_mut_obj_add_obj(doc, field_obj, "value");
			yyjson_mut_obj_add_strcpy(doc, val_obj, "type",
			                          IcebergTypeHelper::LogicalTypeToIcebergType(val_child->type).c_str());
			auto nested_key_fields_arr = yyjson_mut_obj_add_arr(doc, val_obj, "fields");
			AddNamedField(doc, nested_key_fields_arr, *val_child);
		}
		yyjson_mut_obj_add_uint(doc, field_obj, "value-id", val_child->id);
		yyjson_mut_obj_add_bool(doc, field_obj, "value-required", false);
		break;
	}
	default:
		throw NotImplementedException("Unrecognized nested type %s", LogicalTypeIdToString(column.type.id()));
	}
	// skip doc, initial_default, and write_default for now.
	//	yyjson_mut_obj_add_strcpy(doc, field_obj, "doc", "string");
	//	yyjson_mut_obj_add_bool(doc, field_obj, "initial_default", true);
	//	yyjson_mut_obj_add_bool(doc, field_obj, "write_default", true);
}

shared_ptr<IcebergTableSchema> IcebergCreateTableRequest::CreateIcebergSchema(const ICTableEntry *table_entry) {
	auto schema = make_shared_ptr<IcebergTableSchema>();
	// should this be a different schema id?
	schema->schema_id = table_entry->table_info.table_metadata.current_schema_id;

	// TODO: this can all be refactored out
	//  this makes the IcebergTableSchema, and we use that to dump data to JSON.
	//  we can just directly dump it to json.
	auto column_iterator = table_entry->GetColumns().Logical();
	idx_t field_id = 1;

	auto next_field_id = [&field_id]() -> idx_t {
		return field_id++;
	};

	for (auto column = column_iterator.begin(); column != column_iterator.end(); ++column) {
		auto name = (*column).Name();
		// TODO: is this correct?
		bool required = false;
		auto logical_type = (*column).GetType();
		idx_t first_id = next_field_id();
		rest_api_objects::Type type;
		if (logical_type.IsNested()) {
			type = IcebergTypeHelper::CreateIcebergRestType(logical_type, next_field_id);
		} else {
			type.has_primitive_type = true;
			type.primitive_type = rest_api_objects::PrimitiveType();
			type.primitive_type.value = IcebergTypeHelper::LogicalTypeToIcebergType(logical_type);
		}
		auto column_def = IcebergColumnDefinition::ParseType(name, first_id, required, type, nullptr);
		schema->columns.push_back(std::move(column_def));
	}
	return schema;
}

string IcebergCreateTableRequest::CreateTableToJSON(std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p) {
	auto doc = doc_p.get();
	auto root_object = yyjson_mut_doc_get_root(doc);

	auto schema = initial_schema;

	//! name
	yyjson_mut_obj_add_strcpy(doc, root_object, "name", table_name.c_str());
	//! location (apparently not needed)

	auto schema_json = yyjson_mut_obj_add_obj(doc, root_object, "schema");

	yyjson_mut_obj_add_strcpy(doc, schema_json, "type", "struct");
	auto fields_arr = yyjson_mut_obj_add_arr(doc, schema_json, "fields");

	// populate the fields
	for (auto &field : schema->columns) {
		auto field_obj = yyjson_mut_arr_add_obj(doc, fields_arr);
		// add name and id for top level items immediately
		AddNamedField(doc, field_obj, *field);
	}

	yyjson_mut_obj_add_uint(doc, schema_json, "schema-id", schema->schema_id);
	auto identifier_fields_arr = yyjson_mut_obj_add_arr(doc, schema_json, "identifier-field-ids");

	auto partition_spec = yyjson_mut_obj_add_obj(doc, root_object, "partition-spec");
	yyjson_mut_obj_add_uint(doc, partition_spec, "spec-id", 0);
	auto partition_spec_fields = yyjson_mut_obj_add_arr(doc, partition_spec, "fields");

	auto write_order = yyjson_mut_obj_add_obj(doc, root_object, "write-order");
	yyjson_mut_obj_add_uint(doc, write_order, "order-id", 0);
	auto write_order_fields = yyjson_mut_obj_add_arr(doc, write_order, "fields");
	auto properties = yyjson_mut_obj_add_obj(doc, root_object, "properties");

	return ICUtils::JsonToString(std::move(doc_p));
}

} // namespace duckdb
