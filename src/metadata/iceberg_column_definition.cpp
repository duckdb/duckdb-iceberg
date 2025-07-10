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
                                   optional_ptr<rest_api_objects::PrimitiveTypeValue> initial_default) {
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
			auto child = ParseType(field.name, field.id, field.required, *field.type,
			                       field.has_initial_default ? &field.initial_default : nullptr);
			struct_children.push_back(std::make_pair(child->name, child->type));
			res->children.push_back(std::move(child));
		}
		res->type = LogicalType::STRUCT(std::move(struct_children));
	} else if (type.has_list_type) {
		auto &list_type = type.list_type;
		auto child =
		    ParseType("element", list_type.element_id, list_type.element_required, *list_type.element, nullptr);
		res->type = LogicalType::LIST(child->type);
		res->children.push_back(std::move(child));
	} else if (type.has_map_type) {
		auto &map_type = type.map_type;
		auto key = ParseType("key", map_type.key_id, true, *map_type.key, nullptr);
		auto value = ParseType("value", map_type.value_id, map_type.value_required, *map_type.value, nullptr);
		res->type = LogicalType::MAP(key->type, value->type);
		res->children.push_back(std::move(key));
		res->children.push_back(std::move(value));
	} else {
		throw InvalidConfigurationException("Encountered an invalid type in JSON schema");
	}

	if (initial_default) {
		res->initial_default = ParseDefaultForType(res->type, *initial_default);
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
	throw InvalidConfigurationException("Unrecognized primitive type: %s", type_str);
}

unique_ptr<IcebergColumnDefinition> IcebergColumnDefinition::ParseStructField(rest_api_objects::StructField &field) {
	return ParseType(field.name, field.id, field.required, *field.type,
	                 field.has_initial_default ? &field.initial_default : nullptr);
}

bool IcebergColumnDefinition::IsPrimitiveType() {
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
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
