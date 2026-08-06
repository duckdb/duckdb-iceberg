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

namespace duckdb {

IcebergCreateTableRequest::IcebergCreateTableRequest(string name_p, shared_ptr<IcebergTableSchema> schema_p,
                                                     IcebergPartitionSpec partition_spec_p,
                                                     IcebergSortOrder sort_order_p, idx_t iceberg_version_p,
                                                     case_insensitive_map_t<string> table_properties_p,
                                                     string location_p)
    : name(std::move(name_p)), schema(std::move(schema_p)), partition_spec(std::move(partition_spec_p)),
      sort_order(std::move(sort_order_p)), iceberg_version(iceberg_version_p),
      table_properties(std::move(table_properties_p)), location(std::move(location_p)) {
}

static void AddUnnamedField(JSONWriter &writer, JSONMutableValue field_obj, const IcebergColumnDefinition &column);

static void AddNamedField(JSONWriter &writer, JSONMutableValue field_obj, const IcebergColumnDefinition &column) {
	field_obj.AddString("name", column.name);
	field_obj.Add("id", writer.CreateUnsignedInteger(column.id));
	field_obj.Add("required", writer.CreateBoolean(column.required));
	if (column.doc) {
		field_obj.AddString("doc", *column.doc);
	}

	if (column.type.id() != LogicalTypeId::VARIANT && column.type.IsNested()) {
		auto type_obj = writer.CreateObject();
		field_obj.Add("type", type_obj);
		AddUnnamedField(writer, type_obj, column);
		//! Add default as empty object: '{}'
		if (column.initial_default && !column.initial_default->IsNull()) {
			field_obj.Add("initial-default", writer.CreateObject());
		}
		if (column.write_default && !column.write_default->IsNull()) {
			field_obj.Add("write-default", writer.CreateObject());
		}
		return;
	}

	//! Write of non-struct type
	field_obj.AddString("type", IcebergTypeHelper::LogicalTypeToIcebergType(column.type));
	if (column.initial_default && !column.initial_default->IsNull()) {
		auto primitive_type_value = IcebergTypeHelper::PrimitiveTypeFromValue(*column.initial_default);
		field_obj.Add("initial-default", IcebergTypeHelper::PrimitiveTypeValueToJSON(writer, primitive_type_value));
	}
	if (column.write_default && !column.write_default->IsNull()) {
		auto primitive_type_value = IcebergTypeHelper::PrimitiveTypeFromValue(*column.write_default);
		field_obj.Add("write-default", IcebergTypeHelper::PrimitiveTypeValueToJSON(writer, primitive_type_value));
	}
}

static void AddUnnamedField(JSONWriter &writer, JSONMutableValue field_obj, const IcebergColumnDefinition &column) {
	D_ASSERT(column.type.IsNested());
	switch (column.type.id()) {
	case LogicalTypeId::STRUCT: {
		field_obj.AddString("type", "struct");
		auto nested_fields_arr = writer.CreateArray();
		field_obj.Add("fields", nested_fields_arr);
		for (idx_t i = 0; i < column.GetChildCount(); i++) {
			auto field = column.GetChild(i);
			auto nested_field_obj = writer.CreateObject();
			nested_fields_arr.Append(nested_field_obj);
			AddNamedField(writer, nested_field_obj, *field);
		}
		break;
	}
	case LogicalTypeId::LIST: {
		field_obj.AddString("type", "list");
		D_ASSERT(column.GetChildCount() == 1);
		auto list_type = column.GetChild("element");
		field_obj.Add("element-id", writer.CreateUnsignedInteger(list_type->id));
		if (list_type->IsIcebergPrimitiveType()) {
			field_obj.AddString("element", IcebergTypeHelper::LogicalTypeToIcebergType(list_type->type));
		} else {
			auto list_type_obj = writer.CreateObject();
			field_obj.Add("element", list_type_obj);
			AddUnnamedField(writer, list_type_obj, *list_type);
		}
		field_obj.Add("element-required", writer.CreateBoolean(false));
		return;
	}
	case LogicalTypeId::MAP: {
		field_obj.AddString("type", "map");
		D_ASSERT(column.GetChildCount() == 2);
		auto key_child = column.GetChild("key");
		if (key_child->IsIcebergPrimitiveType()) {
			field_obj.AddString("key", IcebergTypeHelper::LogicalTypeToIcebergType(key_child->type));
		} else {
			auto key_obj = writer.CreateObject();
			field_obj.Add("key", key_obj);
			AddUnnamedField(writer, key_obj, *key_child);
		}
		field_obj.Add("key-id", writer.CreateUnsignedInteger(key_child->id));
		auto val_child = column.GetChild("value");
		if (val_child->IsIcebergPrimitiveType()) {
			field_obj.AddString("value", IcebergTypeHelper::LogicalTypeToIcebergType(val_child->type));
		} else {
			auto val_obj = writer.CreateObject();
			field_obj.Add("value", val_obj);
			AddUnnamedField(writer, val_obj, *val_child);
		}
		field_obj.Add("value-id", writer.CreateUnsignedInteger(val_child->id));
		field_obj.Add("value-required", writer.CreateBoolean(false));
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

void IcebergCreateTableRequest::PopulateSchema(JSONWriter &writer, JSONMutableValue schema_json,
                                               const IcebergTableSchema &schema) {
	schema_json.AddString("type", "struct");
	auto fields_arr = writer.CreateArray();
	schema_json.Add("fields", fields_arr);

	for (auto &field : schema.columns) {
		auto field_obj = writer.CreateObject();
		fields_arr.Append(field_obj);
		// top level fields are always named
		AddNamedField(writer, field_obj, *field);
	}

	schema_json.Add("schema-id", writer.CreateUnsignedInteger(schema.schema_id));
	if (!schema.identifier_field_ids.empty()) {
		auto identifier_field_ids = writer.CreateArray();
		schema_json.Add("identifier-field-ids", identifier_field_ids);
		for (const auto field_id : schema.identifier_field_ids) {
			identifier_field_ids.Append(writer.CreateSignedInteger(field_id));
		}
	}
}

string IcebergCreateTableRequest::CreateTableToJSON(bool stage_create) const {
	JSONWriter writer;
	auto root_object = writer.CreateObject();
	writer.SetRoot(root_object);

	// If stage create is supported, create the table with stage_create = true and the table update will
	// commit the table.
	root_object.Add("stage-create", writer.CreateBoolean(stage_create));
	root_object.AddString("name", name);
	auto schema_json = writer.CreateObject();
	root_object.Add("schema", schema_json);
	if (!schema) {
		throw InternalException("Attempted to create a CreateTableRequest without a schema payload");
	}
	PopulateSchema(writer, schema_json, *schema);

	auto partition_spec_json = writer.CreateObject();
	root_object.Add("partition-spec", partition_spec_json);
	partition_spec_json.Add("spec-id", writer.CreateUnsignedInteger(0));
	partition_spec_json.AddString("type", "struct");
	auto fields_arr = writer.CreateArray();
	partition_spec_json.Add("fields", fields_arr);

	for (auto &field : partition_spec.fields) {
		auto field_obj = writer.CreateObject();
		fields_arr.Append(field_obj);
		field_obj.AddString("name", field.GetPartitionSpecFieldName());
		field_obj.AddString("transform", field.transform.RawType());
		field_obj.Add("source-id", writer.CreateSignedInteger(field.source_id));
		field_obj.Add("field-id", writer.CreateSignedInteger(field.partition_field_id));
	}

	root_object.Add("write-order", sort_order.ToJSON(writer));

	auto properties = writer.CreateObject();
	root_object.Add("properties", properties);
	properties.AddString("format-version", std::to_string(iceberg_version));
	for (auto &property : table_properties) {
		properties.AddString(property.first, property.second);
	}
	if (!location.empty()) {
		root_object.AddString("location", location);
	}
	return writer.ToString(JSONWriteFlags::ALLOW_INF_AND_NAN);
}

} // namespace duckdb
