#include "metadata/iceberg_column_definition.hpp"

namespace duckdb {

// https://iceberg.apache.org/spec/#schemas

//! Hexadecimal values are given without the proper escape sequences, so we add them, for simplicity of conversion
static string AddEscapesToBlob(const string &hexadecimal_string) {
	string result;
	D_ASSERT(hexadecimal_string.size() % 2 == 0);
	for (idx_t i = 0; i < hexadecimal_string.size() / 2; i++) {
		result += "\\x";
		result += hexadecimal_string.substr(i * 2, 2);
	}
	return result;
}

static Value ParseDefaultForType(const LogicalType &type, rest_api_objects::PrimitiveTypeValue &default_value) {
	if (type.IsNested()) {
		throw InvalidConfigurationException("Can't parse default value for nested type (%s)", type.ToString());
	}

	switch (type.id()) {
	case LogicalTypeId::BOOLEAN: {
		D_ASSERT(default_value.has_boolean_type_value);
		return Value::BOOLEAN(default_value.boolean_type_value.value);
	}
	case LogicalTypeId::INTEGER: {
		D_ASSERT(default_value.has_integer_type_value);
		return Value::INTEGER(default_value.integer_type_value.value);
	}
	case LogicalTypeId::BIGINT: {
		D_ASSERT(default_value.has_long_type_value);
		return Value::BIGINT(default_value.long_type_value.value);
	}
	case LogicalTypeId::FLOAT: {
		D_ASSERT(default_value.has_float_type_value);
		return Value::FLOAT(default_value.float_type_value.value);
	}
	case LogicalTypeId::DOUBLE: {
		D_ASSERT(default_value.has_double_type_value);
		return Value::DOUBLE(default_value.double_type_value.value);
	}
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::UUID: {
		D_ASSERT(default_value.has_string_type_value);
		return Value(default_value.string_type_value.value).DefaultCastAs(type);
	}
	case LogicalTypeId::BLOB: {
		D_ASSERT(default_value.has_binary_type_value);
		return Value::BLOB(AddEscapesToBlob(default_value.binary_type_value.value));
	}
	default:
		throw NotImplementedException("ParseDefaultForType not implemented for type: %s", type.ToString());
	}
}

unique_ptr<IcebergColumnDefinition>
IcebergColumnDefinition::ParseType(const string &name, int32_t field_id, bool required, rest_api_objects::Type &type,
                                   optional_ptr<rest_api_objects::PrimitiveTypeValue> initial_default,
                                   optional_ptr<rest_api_objects::PrimitiveTypeValue> write_default) {
	auto res = make_uniq<IcebergColumnDefinition>();
	res->id = field_id;
	res->required = required;
	res->name = name;

	if (type.has_primitive_type) {
		res->type = ParsePrimitiveType(type.primitive_type);
	} else if (type.has_struct_type) {
		auto &struct_type = type.struct_type;
		child_list_t<LogicalType> struct_children;
		for (auto &field_p : struct_type.fields) {
			auto &field = *field_p;
			auto child = ParseStructField(field);
			struct_children.push_back(std::make_pair(child->name, child->type));
			res->children.push_back(std::move(child));
		}
		res->type = LogicalType::STRUCT(std::move(struct_children));
	} else if (type.has_list_type) {
		auto &list_type = type.list_type;
		auto child = ParseType("element", list_type.element_id, list_type.element_required, *list_type.element, nullptr,
		                       nullptr);
		res->type = LogicalType::LIST(child->type);
		res->children.push_back(std::move(child));
	} else if (type.has_map_type) {
		auto &map_type = type.map_type;
		auto key = ParseType("key", map_type.key_id, true, *map_type.key, nullptr);
		auto value = ParseType("value", map_type.value_id, map_type.value_required, *map_type.value, nullptr, nullptr);
		res->type = LogicalType::MAP(key->type, value->type);
		res->children.push_back(std::move(key));
		res->children.push_back(std::move(value));
	} else {
		throw InvalidConfigurationException("Encountered an invalid type in JSON schema");
	}

	if (initial_default) {
		res->initial_default = make_uniq<Value>(ParseDefaultForType(res->type, *initial_default));
	}
	if (write_default) {
		res->write_default = make_uniq<Value>(ParseDefaultForType(res->type, *write_default));
	}
	return res;
}

LogicalType IcebergColumnDefinition::ParsePrimitiveType(rest_api_objects::PrimitiveType &type) {
	auto &type_str = type.value;

	return ParsePrimitiveTypeString(type_str);
}

LogicalType IcebergColumnDefinition::ParsePrimitiveTypeString(const string &type_str) {
	if (type_str == "boolean") {
		return LogicalType::BOOLEAN;
	}
	if (type_str == "int") {
		return LogicalType::INTEGER;
	}
	if (type_str == "long") {
		return LogicalType::BIGINT;
	}
	if (type_str == "float") {
		return LogicalType::FLOAT;
	}
	if (type_str == "double") {
		return LogicalType::DOUBLE;
	}
	if (type_str == "date") {
		return LogicalType::DATE;
	}
	if (type_str == "time") {
		return LogicalType::TIME;
	}
	if (type_str == "timestamp") {
		return LogicalType::TIMESTAMP;
	}
	if (type_str == "timestamptz") {
		return LogicalType::TIMESTAMP_TZ;
	}
	if (type_str == "string") {
		return LogicalType::VARCHAR;
	}
	if (type_str == "uuid") {
		return LogicalType::UUID;
	}
	if (StringUtil::StartsWith(type_str, "fixed")) {
		// FIXME: use fixed size type in DuckDB
		return LogicalType::BLOB;
	}
	if (type_str == "binary") {
		return LogicalType::BLOB;
	}
	if (StringUtil::StartsWith(type_str, "decimal")) {
		D_ASSERT(type_str[7] == '(');
		D_ASSERT(type_str.back() == ')');
		auto start = type_str.find('(');
		auto end = type_str.rfind(')');
		auto raw_digits = type_str.substr(start + 1, end - start);
		auto digits = StringUtil::Split(raw_digits, ',');
		D_ASSERT(digits.size() == 2);

		auto width = std::stoi(digits[0]);
		auto scale = std::stoi(digits[1]);
		return LogicalType::DECIMAL(width, scale);
	}
	if (type_str == "variant") {
		return LogicalType::VARIANT();
	}
	throw InvalidConfigurationException("Unrecognized primitive type: %s", type_str);
}

unique_ptr<IcebergColumnDefinition> IcebergColumnDefinition::ParseStructField(rest_api_objects::StructField &field) {
	auto field_initial_default = field.has_initial_default ? &field.initial_default : nullptr;
	auto field_write_default = field.has_write_default ? &field.write_default : nullptr;
	return ParseType(field.name, field.id, field.required, *field.type, field_initial_default, field_write_default);
}

bool IcebergColumnDefinition::IsIcebergPrimitiveType() const {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::DATE:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::UUID:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::VARIANT:
		return true;
	default:
		return false;
	}
}

std::string PrimitiveTypeToJson(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return "boolean";
	case LogicalTypeId::INTEGER:
		return "int";
	case LogicalTypeId::BIGINT:
		return "long";
	case LogicalTypeId::FLOAT:
		return "float";
	case LogicalTypeId::DOUBLE:
		return "double";
	case LogicalTypeId::DATE:
		return "date";
	case LogicalTypeId::TIME:
		return "time";
	case LogicalTypeId::TIMESTAMP:
		return "timestamp";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "timestamptz";
	case LogicalTypeId::VARCHAR:
		return "string";
	case LogicalTypeId::UUID:
		return "uuid";
	case LogicalTypeId::BLOB:
		return "binary";
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		return "decimal(" + std::to_string(width) + "," + std::to_string(scale) + ")";
	}	
	default:
		throw InvalidConfigurationException("Unrecognized primitive type: %s", type.ToString());
	}
}

void DefaultToJson(yyjson_mut_doc *doc, yyjson_mut_val *default_obj, const Value &default_value) {
	switch (default_value.type().id()) {
	case LogicalTypeId::BOOLEAN:
		yyjson_mut_obj_add_bool(doc, default_obj, "default", default_value.GetValueUnsafe<bool>());
		break;
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		yyjson_mut_obj_add_int(doc, default_obj, "default", default_value.GetValue<int64_t>());
		break;
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		yyjson_mut_obj_add_real(doc, default_obj, "default", default_value.GetValue<double>());
		break;
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::UUID:
		yyjson_mut_obj_add_strcpy(doc, default_obj, "default",
		                          default_value.ToString().c_str());
		break;
	default:
		throw NotImplementedException("DefaultToJson not implemented for type: %s", default_value.type().ToString());
	}
}

static void TypeToJson(const IcebergColumnDefinition &column, yyjson_mut_doc *doc, yyjson_mut_val *field_obj, const char *key) {
	if (column.IsIcebergPrimitiveType()) {
		yyjson_mut_obj_add_strcpy(doc, field_obj, key, PrimitiveTypeToJson(column.type).c_str());
		return;
	}
	auto type_obj = yyjson_mut_obj_add_obj(doc, field_obj, key);
	if (column.type.id() == LogicalTypeId::STRUCT) {
		yyjson_mut_obj_add_strcpy(doc, type_obj, "type", "struct");
		auto fields_arr = yyjson_mut_obj_add_arr(doc, type_obj, "fields");
		for (auto &child : column.children) {
			auto child_field_obj = yyjson_mut_arr_add_obj(doc, fields_arr);
			yyjson_mut_obj_add_uint(doc, child_field_obj, "id", child->id);
			yyjson_mut_obj_add_strcpy(doc, child_field_obj, "name", child->name.c_str());
			yyjson_mut_obj_add_bool(doc, child_field_obj, "required", child->required);
			TypeToJson(*child, doc, child_field_obj, "type");
		}
	} else if (column.type.id() == LogicalTypeId::LIST) {
		yyjson_mut_obj_add_strcpy(doc, type_obj, "type", "list");
		auto &child = column.children[0];
		yyjson_mut_obj_add_uint(doc, type_obj, "element-id", child->id);
		yyjson_mut_obj_add_bool(doc, type_obj, "element-required", child->required);
		TypeToJson(*child, doc, type_obj, "element");
	} else if (column.type.id() == LogicalTypeId::MAP) {
		yyjson_mut_obj_add_strcpy(doc, type_obj, "type", "map");
		auto &key_child = column.children[0];
		auto &val_child = column.children[1];
		yyjson_mut_obj_add_uint(doc, type_obj, "key-id", key_child->id);
		TypeToJson(*key_child, doc, type_obj, "key");
		yyjson_mut_obj_add_uint(doc, type_obj, "value-id", val_child->id);
		TypeToJson(*val_child, doc, type_obj, "value");
		yyjson_mut_obj_add_bool(doc, type_obj, "value-required", val_child->required);
	} else {
		throw NotImplementedException("TypeToJson not implemented for type: %s", column.type.ToString());
	}
}

void IcebergColumnDefinition::ToJson(yyjson_mut_doc *doc, yyjson_mut_val *field_obj) const {
	yyjson_mut_obj_add_uint(doc, field_obj, "id", id);
	yyjson_mut_obj_add_strcpy(doc, field_obj, "name", name.c_str());
	yyjson_mut_obj_add_bool(doc, field_obj, "required", required);
	TypeToJson(*this, doc, field_obj, "type");
	if (write_default) {
		DefaultToJson(doc, field_obj, *write_default);
	} else if (initial_default) {
		DefaultToJson(doc, field_obj, *initial_default);
	}
}

ColumnDefinition IcebergColumnDefinition::GetColumnDefinition() const {
	optional_ptr<Value> default_to_use;
	if (write_default) {
		//! Use write-default if it's set
		default_to_use = write_default.get();
	} else if (initial_default) {
		//! If it's not set, use the initial-default (if that *is* set)
		default_to_use = initial_default.get();
	}
	if (default_to_use) {
		//! FIXME: the expression needs to be more advanced for nested types
		if (type.IsNested()) {
			throw NotImplementedException("DEFAULT values for nested types are not supported currently");
		}
		return ColumnDefinition(name, type, make_uniq<ConstantExpression>(*default_to_use), TableColumnType::STANDARD);
	} else {
		return ColumnDefinition(name, type);
	}
}

} // namespace duckdb
