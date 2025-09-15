#include "storage/table_update/iceberg_add_snapshot.hpp"
#include "storage/table_create/iceberg_create_table_request.hpp"
#include "storage/irc_table_set.hpp"
#include "storage/iceberg_table_information.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
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
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type", "list");
		D_ASSERT(column.children.size() == 1);
		auto &list_type = column.children[0];
		yyjson_mut_obj_add_uint(doc, field_obj, "element-id", list_type->id);
		if (list_type->IsIcebergPrimitiveType()) {
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
		if (key_child->IsIcebergPrimitiveType()) {
			yyjson_mut_obj_add_strcpy(doc, field_obj, "key",
			                          IcebergTypeHelper::LogicalTypeToIcebergType(key_child->type).c_str());
		} else {
			auto key_obj = yyjson_mut_obj_add_obj(doc, field_obj, "key");
			AddUnnamedField(doc, key_obj, *key_child);
		}
		yyjson_mut_obj_add_uint(doc, field_obj, "key-id", key_child->id);
		auto &val_child = column.children[1];
		if (val_child->IsIcebergPrimitiveType()) {
			yyjson_mut_obj_add_strcpy(doc, field_obj, "value",
			                          IcebergTypeHelper::LogicalTypeToIcebergType(val_child->type).c_str());
		} else {
			auto val_obj = yyjson_mut_obj_add_obj(doc, field_obj, "value");
			AddUnnamedField(doc, val_obj, *val_child);
		}
		yyjson_mut_obj_add_uint(doc, field_obj, "value-id", val_child->id);
		yyjson_mut_obj_add_bool(doc, field_obj, "value-required", false);
		break;
	}
	default:
		throw NotImplementedException("Unrecognized nested type %s", LogicalTypeIdToString(column.type.id()));
	}
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

	auto &constraints = table_entry->GetConstraints();
	for (auto column = column_iterator.begin(); column != column_iterator.end(); ++column) {
		auto name = (*column).Name();
		// check if there is a not null constraint
		bool required = false;
		if (!constraints.empty()) {
			for (auto &constraint : constraints) {
				if (constraint->type != ConstraintType::NOT_NULL) {
					continue;
				}
				auto &not_null_constraint = constraint->Cast<NotNullConstraint>();
				if (not_null_constraint.index.IsValid() && not_null_constraint.index.index == column.pos) {
					required = true;
				}
			}
		}

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

void IcebergCreateTableRequest::PopulateSchema(yyjson_mut_doc *doc, yyjson_mut_val *schema_json,
                                               IcebergTableSchema &schema) {
	yyjson_mut_obj_add_strcpy(doc, schema_json, "type", "struct");
	auto fields_arr = yyjson_mut_obj_add_arr(doc, schema_json, "fields");

	for (auto &field : schema.columns) {
		auto field_obj = yyjson_mut_arr_add_obj(doc, fields_arr);
		// top level fields are always named
		AddNamedField(doc, field_obj, *field);
	}

	yyjson_mut_obj_add_uint(doc, schema_json, "schema-id", schema.schema_id);
}

string IcebergCreateTableRequest::CreateTableToJSON(std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p) {
	auto doc = doc_p.get();
	auto root_object = yyjson_mut_doc_get_root(doc);

	yyjson_mut_obj_add_strcpy(doc, root_object, "name", table_name.c_str());
	auto schema_json = yyjson_mut_obj_add_obj(doc, root_object, "schema");

	PopulateSchema(doc, schema_json, *initial_schema.get());

	auto partition_spec = yyjson_mut_obj_add_obj(doc, root_object, "partition-spec");
	yyjson_mut_obj_add_uint(doc, partition_spec, "spec-id", 0);
	auto partition_spec_fields = yyjson_mut_obj_add_arr(doc, partition_spec, "fields");

	auto write_order = yyjson_mut_obj_add_obj(doc, root_object, "write-order");
	yyjson_mut_obj_add_uint(doc, write_order, "order-id", 0);
	// unused, but we want to add teh objects
	auto write_order_fields = yyjson_mut_obj_add_arr(doc, write_order, "fields");
	auto properties = yyjson_mut_obj_add_obj(doc, root_object, "properties");

	return ICUtils::JsonToString(std::move(doc_p));
}

} // namespace duckdb
