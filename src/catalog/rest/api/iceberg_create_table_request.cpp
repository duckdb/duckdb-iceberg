#include "catalog/rest/api/iceberg_create_table_request.hpp"

#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include "catalog/rest/api/iceberg_add_snapshot.hpp"
#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "catalog/rest/api/iceberg_type.hpp"
#include "common/iceberg_default.hpp"

using namespace duckdb_yyjson;
namespace duckdb {

IcebergCreateTableRequest::IcebergCreateTableRequest(string name_p, shared_ptr<IcebergTableSchema> schema_p,
                                                     IcebergPartitionSpec partition_spec_p, idx_t iceberg_version_p,
                                                     case_insensitive_map_t<string> table_properties_p,
                                                     string location_p)
    : name(std::move(name_p)), schema(std::move(schema_p)), partition_spec(std::move(partition_spec_p)),
      iceberg_version(iceberg_version_p), table_properties(std::move(table_properties_p)),
      location(std::move(location_p)) {
}

static void AddUnnamedField(yyjson_mut_doc *doc, yyjson_mut_val *field_obj, const IcebergColumnDefinition &column);

static void AddNamedField(yyjson_mut_doc *doc, yyjson_mut_val *field_obj, const IcebergColumnDefinition &column) {
	yyjson_mut_obj_add_strcpy(doc, field_obj, "name", column.name.c_str());
	yyjson_mut_obj_add_uint(doc, field_obj, "id", column.id);
	yyjson_mut_obj_add_bool(doc, field_obj, "required", column.required);
	if (column.doc) {
		yyjson_mut_obj_add_strcpy(doc, field_obj, "doc", column.doc->c_str());
	}

	if (column.type.id() != LogicalTypeId::VARIANT && column.type.IsNested()) {
		auto type_obj = yyjson_mut_obj_add_obj(doc, field_obj, "type");
		AddUnnamedField(doc, type_obj, column);
		//! Add default as empty object: '{}'
		if (column.initial_default && !column.initial_default->IsNull()) {
			(void)yyjson_mut_obj_add_obj(doc, field_obj, "initial-default");
		}
		if (column.write_default && !column.write_default->IsNull()) {
			(void)yyjson_mut_obj_add_obj(doc, field_obj, "write-default");
		}
		return;
	}

	//! Write of non-struct type
	yyjson_mut_obj_add_strcpy(doc, field_obj, "type", IcebergTypeHelper::LogicalTypeToIcebergType(column.type).c_str());
	if (column.initial_default && !column.initial_default->IsNull()) {
		auto primitive_type_value = IcebergTypeHelper::PrimitiveTypeFromValue(*column.initial_default);
		yyjson_mut_obj_add_val(doc, field_obj, "initial-default",
		                       IcebergTypeHelper::PrimitiveTypeValueToJSON(doc, primitive_type_value));
	}
	if (column.write_default && !column.write_default->IsNull()) {
		auto primitive_type_value = IcebergTypeHelper::PrimitiveTypeFromValue(*column.write_default);
		yyjson_mut_obj_add_val(doc, field_obj, "write-default",
		                       IcebergTypeHelper::PrimitiveTypeValueToJSON(doc, primitive_type_value));
	}
}

static void AddUnnamedField(yyjson_mut_doc *doc, yyjson_mut_val *field_obj, const IcebergColumnDefinition &column) {
	D_ASSERT(column.type.IsNested());
	switch (column.type.id()) {
	case LogicalTypeId::STRUCT: {
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type", "struct");
		auto nested_fields_arr = yyjson_mut_obj_add_arr(doc, field_obj, "fields");
		for (idx_t i = 0; i < column.GetChildCount(); i++) {
			auto field = column.GetChild(i);
			auto nested_field_obj = yyjson_mut_arr_add_obj(doc, nested_fields_arr);
			AddNamedField(doc, nested_field_obj, *field);
		}
		break;
	}
	case LogicalTypeId::LIST: {
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type", "list");
		D_ASSERT(column.GetChildCount() == 1);
		auto list_type = column.GetChild("element");
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
		D_ASSERT(column.GetChildCount() == 2);
		auto key_child = column.GetChild("key");
		if (key_child->IsIcebergPrimitiveType()) {
			yyjson_mut_obj_add_strcpy(doc, field_obj, "key",
			                          IcebergTypeHelper::LogicalTypeToIcebergType(key_child->type).c_str());
		} else {
			auto key_obj = yyjson_mut_obj_add_obj(doc, field_obj, "key");
			AddUnnamedField(doc, key_obj, *key_child);
		}
		yyjson_mut_obj_add_uint(doc, field_obj, "key-id", key_child->id);
		auto val_child = column.GetChild("value");
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

unique_ptr<IcebergColumnDefinition>
IcebergCreateTableRequest::CreateIcebergColumn(const ColumnDefinition &column_def, IcebergDefaultBinder &default_binder,
                                               bool required, const std::function<idx_t(void)> &next_field_id,
                                               idx_t iceberg_version) {
	const auto &name = column_def.Name();
	const auto &logical_type = column_def.GetType();

	Value default_value;
	if (column_def.HasDefaultValue()) {
		auto &default_expr = column_def.DefaultValue();
		default_value = default_binder.Evaluate(default_expr, logical_type);
		if (iceberg_version < 3 && !default_value.IsNull()) {
			throw InvalidInputException("non-null DEFAULT values are not supported for <V3 tables");
		}
	}

	auto rest_type = IcebergTypeHelper::CreateIcebergRestType(name.GetIdentifierName(), logical_type, required, "",
	                                                          default_value, next_field_id, iceberg_version);
	auto iceberg_column_def = IcebergColumnDefinition::ParseStructField(rest_type);
	return iceberg_column_def;
}

shared_ptr<IcebergTableSchema> IcebergCreateTableRequest::CreateIcebergSchema(
    ClientContext &context, const IcebergTableMetadata &table_metadata, const ColumnList &columns,
    optional_ptr<const vector<unique_ptr<Constraint>>> constraints_p, int32_t &last_column_id) {
	auto schema = make_shared_ptr<IcebergTableSchema>();
	schema->schema_id = table_metadata.GetCurrentSchemaId();

	// TODO: this can all be refactored out
	//  this makes the IcebergTableSchema, and we use that to dump data to JSON.
	//  we can just directly dump it to json.
	auto column_iterator = columns.Logical();
	int32_t field_id = 1;

	auto next_field_id = [&field_id]() -> idx_t {
		return field_id++;
	};

	unordered_set<idx_t> required_columns;
	if (constraints_p) {
		auto &constraints = *constraints_p;
		for (auto &constraint : constraints) {
			if (constraint->type != ConstraintType::NOT_NULL) {
				continue;
			}
			auto &not_null_constraint = constraint->Cast<NotNullConstraint>();
			if (!not_null_constraint.index.IsValid()) {
				continue;
			}
			required_columns.insert(not_null_constraint.index.index);
		}
	}

	IcebergDefaultBinder binder(context);
	for (auto column = column_iterator.begin(); column != column_iterator.end(); ++column) {
		auto &column_def = *column;
		const bool required = required_columns.count(column.pos);

		auto iceberg_column_def =
		    CreateIcebergColumn(column_def, binder, required, next_field_id, table_metadata.iceberg_version);
		schema->columns.push_back(std::move(iceberg_column_def));
	}
	last_column_id = field_id - 1;
	return schema;
}

void IcebergCreateTableRequest::PopulateSchema(yyjson_mut_doc *doc, yyjson_mut_val *schema_json,
                                               const IcebergTableSchema &schema) {
	yyjson_mut_obj_add_strcpy(doc, schema_json, "type", "struct");
	auto fields_arr = yyjson_mut_obj_add_arr(doc, schema_json, "fields");

	for (auto &field : schema.columns) {
		auto field_obj = yyjson_mut_arr_add_obj(doc, fields_arr);
		// top level fields are always named
		AddNamedField(doc, field_obj, *field);
	}

	yyjson_mut_obj_add_uint(doc, schema_json, "schema-id", schema.schema_id);
	if (!schema.identifier_field_ids.empty()) {
		auto identifier_field_ids = yyjson_mut_obj_add_arr(doc, schema_json, "identifier-field-ids");
		for (const auto field_id : schema.identifier_field_ids) {
			yyjson_mut_arr_add_int(doc, identifier_field_ids, field_id);
		}
	}
}

string IcebergCreateTableRequest::CreateTableToJSON(bool stage_create) const {
	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
	yyjson_mut_doc *doc = doc_p.get();
	auto root_object = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root_object);

	// If stage create is supported, create the table with stage_create = true and the table update will
	// commit the table.
	yyjson_mut_obj_add_bool(doc, root_object, "stage-create", stage_create);
	yyjson_mut_obj_add_strcpy(doc, root_object, "name", name.c_str());
	auto schema_json = yyjson_mut_obj_add_obj(doc, root_object, "schema");
	if (!schema) {
		throw InternalException("Attempted to create a CreateTableRequest without a schema payload");
	}
	PopulateSchema(doc, schema_json, *schema);

	auto partition_spec_json = yyjson_mut_obj_add_obj(doc, root_object, "partition-spec");
	yyjson_mut_obj_add_uint(doc, partition_spec_json, "spec-id", 0);
	yyjson_mut_obj_add_strcpy(doc, partition_spec_json, "type", "struct");
	auto fields_arr = yyjson_mut_obj_add_arr(doc, partition_spec_json, "fields");

	for (auto &field : partition_spec.fields) {
		auto field_obj = yyjson_mut_arr_add_obj(doc, fields_arr);
		yyjson_mut_obj_add_strcpy(doc, field_obj, "name", field.GetPartitionSpecFieldName().c_str());
		yyjson_mut_obj_add_strcpy(doc, field_obj, "transform", field.transform.RawType().c_str());
		yyjson_mut_obj_add_int(doc, field_obj, "source-id", field.source_id);
		yyjson_mut_obj_add_int(doc, field_obj, "field-id", field.partition_field_id);
	}

	auto write_order = yyjson_mut_obj_add_obj(doc, root_object, "write-order");
	yyjson_mut_obj_add_uint(doc, write_order, "order-id", 0);
	// unused, but we want to add the objects
	auto write_order_fields = yyjson_mut_obj_add_arr(doc, write_order, "fields");
	(void)write_order_fields;

	auto properties = yyjson_mut_obj_add_obj(doc, root_object, "properties");
	yyjson_mut_obj_add_strcpy(doc, properties, "format-version", std::to_string(iceberg_version).c_str());
	for (auto &property : table_properties) {
		yyjson_mut_obj_add_strcpy(doc, properties, property.first.c_str(), property.second.c_str());
	}
	if (!location.empty()) {
		yyjson_mut_obj_add_str(doc, root_object, "location", location.c_str());
	}
	return ICUtils::JsonToString(std::move(doc_p));
}

} // namespace duckdb
