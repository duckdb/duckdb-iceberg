#include "metadata/iceberg_table_schema.hpp"

#include "iceberg_metadata.hpp"
#include "iceberg_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {

shared_ptr<IcebergTableSchema> IcebergTableSchema::ParseSchema(rest_api_objects::Schema &schema) {
	auto res = make_shared_ptr<IcebergTableSchema>();
	res->schema_id = schema.object_1.schema_id;
	for (auto &field : schema.struct_type.fields) {
		res->columns.push_back(IcebergColumnDefinition::ParseStructField(*field));
	}
	return res;
}

void IcebergTableSchema::PopulateSourceIdMap(unordered_map<uint64_t, ColumnIndex> &source_to_column_id,
                                             const vector<unique_ptr<IcebergColumnDefinition>> &columns,
                                             optional_ptr<ColumnIndex> parent) {
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &column = columns[i];

		ColumnIndex new_index;
		if (parent) {
			auto primary = parent->GetPrimaryIndex();
			auto child_indexes = parent->GetChildIndexes();
			child_indexes.push_back(ColumnIndex(i));
			new_index = ColumnIndex(primary, child_indexes);
		} else {
			new_index = ColumnIndex(i);
		}

		PopulateSourceIdMap(source_to_column_id, column->children, new_index);
		source_to_column_id.emplace(static_cast<uint64_t>(column->id), std::move(new_index));
	}
}

const IcebergColumnDefinition &
IcebergTableSchema::GetFromColumnIndex(const vector<unique_ptr<IcebergColumnDefinition>> &columns,
                                       const ColumnIndex &column_index, idx_t depth) {
	auto &child_indexes = column_index.GetChildIndexes();
	auto &selected_index = depth ? child_indexes[depth - 1] : column_index;

	auto index = selected_index.GetPrimaryIndex();
	if (index >= columns.size()) {
		throw InvalidConfigurationException("ColumnIndex out of bounds for columns (index %d, 'columns' size: %d)",
		                                    index, columns.size());
	}
	auto &column = columns[index];
	if (depth == child_indexes.size()) {
		return *column;
	}
	if (column->children.empty()) {
		throw InvalidConfigurationException(
		    "Expected column to have children, ColumnIndex has a depth of %d, we reached only %d",
		    column_index.ChildIndexCount(), depth);
	}
	return GetFromColumnIndex(column->children, column_index, depth + 1);
}

static void AddUnnamedField(yyjson_mut_doc *doc, yyjson_mut_val *field_obj, rest_api_objects::Type &column);

static void AddStructField(yyjson_mut_doc *doc, yyjson_mut_val *field_obj, rest_api_objects::StructField &column) {
	yyjson_mut_obj_add_strcpy(doc, field_obj, "name", column.name.c_str());
	yyjson_mut_obj_add_uint(doc, field_obj, "id", column.id);
	if (!column.type->has_primitive_type) {
		auto type_obj = yyjson_mut_obj_add_obj(doc, field_obj, "type");
		AddUnnamedField(doc, type_obj, *column.type);
		yyjson_mut_obj_add_bool(doc, field_obj, "required", column.required);
		return;
	}
	yyjson_mut_obj_add_strcpy(doc, field_obj, "type", column.type->primitive_type.value.c_str());
	yyjson_mut_obj_add_bool(doc, field_obj, "required", column.required);
}

static void AddUnnamedField(yyjson_mut_doc *doc, yyjson_mut_val *field_obj, rest_api_objects::Type &column) {
	if (column.has_struct_type) {
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type", "struct");
		auto nested_fields_arr = yyjson_mut_obj_add_arr(doc, field_obj, "fields");
		for (auto &field : column.struct_type.fields) {
			auto nested_field_obj = yyjson_mut_arr_add_obj(doc, nested_fields_arr);
			AddStructField(doc, nested_field_obj, *field);
		}
	} else if (column.has_list_type) {
		auto type_obj = yyjson_mut_obj_add_obj(doc, field_obj, "type");
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type", "list");
		auto &list_type = column.list_type;
		yyjson_mut_obj_add_uint(doc, field_obj, "element-id", list_type.element_id);
		if (list_type.element->has_primitive_type) {
			yyjson_mut_obj_add_strcpy(doc, field_obj, "element", list_type.element->primitive_type.value.c_str());
		} else {
			auto list_type_obj = yyjson_mut_obj_add_obj(doc, field_obj, "element");
			AddUnnamedField(doc, list_type_obj, *list_type.element);
		}
		yyjson_mut_obj_add_bool(doc, field_obj, "element-required", false);
	} else if (column.has_map_type) {
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type", "map");
		yyjson_mut_obj_add_uint(doc, field_obj, "key-id", column.map_type.key_id);
		auto &key_child = column.map_type.key;
		if (key_child->has_primitive_type) {
			yyjson_mut_obj_add_strcpy(doc, field_obj, "key", key_child->primitive_type.value.c_str());
		} else {
			auto key_type_obj = yyjson_mut_obj_add_obj(doc, field_obj, "key");
			AddUnnamedField(doc, key_type_obj, *key_child);
		}

		auto &val_child = column.map_type.value;
		yyjson_mut_obj_add_uint(doc, field_obj, "value-id", column.map_type.value_id);
		yyjson_mut_obj_add_bool(doc, field_obj, "value-required", false);
		if (val_child->has_primitive_type) {
			yyjson_mut_obj_add_strcpy(doc, field_obj, "value", val_child->primitive_type.value.c_str());
		} else {
			auto value_type_obj = yyjson_mut_obj_add_obj(doc, field_obj, "value");
			AddUnnamedField(doc, value_type_obj, *val_child);
		}
	} else {
		throw NotImplementedException("Unrecognized nested type");
	}
}

void IcebergTableSchema::SchemaToJson(yyjson_mut_doc *doc, yyjson_mut_val *root_object,
                                      const rest_api_objects::Schema &schema) {
	yyjson_mut_obj_add_strcpy(doc, root_object, "type", "struct");
	auto fields_arr = yyjson_mut_obj_add_arr(doc, root_object, "fields");

	// populate the fields
	for (auto &field : schema.struct_type.fields) {
		auto field_obj = yyjson_mut_arr_add_obj(doc, fields_arr);
		// add name and id for top level items immediately
		AddStructField(doc, field_obj, *field);
	}

	D_ASSERT(schema.object_1.has_schema_id);
	yyjson_mut_obj_add_uint(doc, root_object, "schema-id", schema.object_1.schema_id);
	yyjson_mut_obj_add_arr(doc, root_object, "identifier-field-ids");
}

} // namespace duckdb
