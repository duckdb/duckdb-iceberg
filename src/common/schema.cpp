#include "iceberg_metadata.hpp"
#include "iceberg_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {

// https://iceberg.apache.org/spec/#schemas

unique_ptr<IcebergColumnDefinition>
IcebergColumnDefinition::ParseType(const string &name, int32_t field_id, bool required, rest_api_objects::Type &type,
                                   optional_ptr<rest_api_objects::PrimitiveTypeValue> initial_default) {
	auto res = make_uniq<IcebergColumnDefinition>();
	res->id = field_id;
	res->required = required;
	res->name = name;
	//! FIXME: parse the 'initial_default' into a Value

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
	return res;
}

LogicalType IcebergColumnDefinition::ParsePrimitiveType(rest_api_objects::PrimitiveType &type) {
	auto &type_str = type.value;

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

static vector<unique_ptr<IcebergColumnDefinition>> ParseSchemaFromJson(yyjson_val *schema_json) {
	auto parsed_schema = rest_api_objects::Schema::FromJSON(schema_json);
	auto &struct_type = parsed_schema.struct_type;

	vector<unique_ptr<IcebergColumnDefinition>> ret;
	for (auto &field : struct_type.fields) {
		ret.push_back(IcebergColumnDefinition::ParseStructField(*field));
	}
	return ret;
}

vector<unique_ptr<IcebergColumnDefinition>> IcebergSnapshot::ParseSchema(vector<yyjson_val *> &schemas,
                                                                         idx_t schema_id) {
	// Multiple schemas can be present in the json metadata 'schemas' list
	for (const auto &schema_ptr : schemas) {
		auto found_schema_id = IcebergUtils::TryGetNumFromObject(schema_ptr, "schema-id");
		if (found_schema_id == schema_id) {
			return ParseSchemaFromJson(schema_ptr);
		}
	}

	throw InvalidConfigurationException("Iceberg schema with schema id " + to_string(schema_id) + " was not found!");
}

} // namespace duckdb
