#include "core/metadata/schema/iceberg_column_definition.hpp"
#include "duckdb/common/types.hpp"
#include "common/iceberg_constants.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"

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

Value IcebergColumnDefinition::ParsePrimitiveValue(const LogicalType &type,
                                                   const rest_api_objects::PrimitiveTypeValue &default_value) {
	if (default_value.null_type_value) {
		return Value(type);
	}
	if (type.IsNested() && type.id() != LogicalTypeId::STRUCT) {
		throw InvalidConfigurationException("Can't parse default value for nested type (%s)", type.ToString());
	}

	switch (type.id()) {
	case LogicalTypeId::BOOLEAN: {
		D_ASSERT(default_value.boolean_type_value);
		return Value::BOOLEAN(default_value.boolean_type_value->value);
	}
	case LogicalTypeId::INTEGER: {
		D_ASSERT(default_value.integer_type_value);
		return Value::INTEGER(default_value.integer_type_value->value);
	}
	case LogicalTypeId::BIGINT: {
		D_ASSERT(default_value.long_type_value);
		return Value::BIGINT(default_value.long_type_value->value);
	}
	case LogicalTypeId::FLOAT: {
		D_ASSERT(default_value.float_type_value);
		return Value::FLOAT(default_value.float_type_value->value);
	}
	case LogicalTypeId::DOUBLE: {
		D_ASSERT(default_value.double_type_value);
		return Value::DOUBLE(default_value.double_type_value->value);
	}
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::UUID: {
		D_ASSERT(default_value.string_type_value);
		return Value(default_value.string_type_value->value).DefaultCastAs(type);
	}
	case LogicalTypeId::BLOB: {
		D_ASSERT(default_value.binary_type_value);
		return Value::BLOB(AddEscapesToBlob(default_value.binary_type_value->value));
	}
	default:
		throw NotImplementedException("ParsePrimitiveValue not implemented for type: %s", type.ToString());
	}
}

LogicalType IcebergColumnDefinition::ParsePrimitiveType(const rest_api_objects::PrimitiveType &type) {
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
	if (type_str == "timestamp_ns") {
		return LogicalType::TIMESTAMP_NS;
	}
	if (type_str == "timestamptz_ns") {
		return LogicalType::TIMESTAMP_TZ_NS;
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
	if (type_str == "unknown") {
		return LogicalType::SQLNULL;
	}
	if (StringUtil::StartsWith(type_str, "geometry")) {
		// Geometry is an Iceberg v3 type stored as WKB binary in parquet.
		// The type string may include a CRS parameter: geometry(<crs>)
		if (type_str == "geometry") {
			// If we use IcebergConstants::DefaultGeometryCRS, file pruning does not work as well
			// since LogicalType::Geometry(<crs>) != LogicalType::Geometry(), so casts get introduced
			// on Iceberg predicates.
			return LogicalType::GEOMETRY();
		}
		if (type_str.size() > 9 && type_str[8] == '(' && type_str.back() == ')') {
			auto crs_str = type_str.substr(9, type_str.size() - 10);
			return LogicalType::GEOMETRY(crs_str);
		}
		throw InvalidConfigurationException("Invalid geometry type format: %s", type_str);
	}
	if (StringUtil::StartsWith(type_str, "geography")) {
		throw NotImplementedException("Geography support");
	}
	throw InvalidConfigurationException("Unrecognized primitive type: %s", type_str);
}

static rest_api_objects::StructField
CreateStructField(const string &name, int32_t field_id, bool required, const rest_api_objects::Type &iceberg_type,
                  const optional<string> &doc = std::nullopt,
                  const optional<rest_api_objects::PrimitiveTypeValue> &initial_default = std::nullopt,
                  const optional<rest_api_objects::PrimitiveTypeValue> &write_default = std::nullopt) {
	rest_api_objects::StructField result;
	result.id = field_id;
	result.name = name;
	result.type = make_uniq<rest_api_objects::Type>(iceberg_type.Copy());
	result.required = required;
	result._doc = doc;
	if (initial_default) {
		result.initial_default = initial_default->Copy();
	}
	if (write_default) {
		result.write_default = write_default->Copy();
	}
	return result;
}

unique_ptr<IcebergColumnDefinition>
IcebergColumnDefinition::ParseStructField(const rest_api_objects::StructField &field) {
	auto res = make_uniq<IcebergColumnDefinition>();
	res->id = field.id;
	res->required = field.required;
	res->name = field.name;
	if (field._doc) {
		res->doc = *field._doc;
	}

	auto &type = *field.type;
	if (type.primitive_type) {
		res->type = ParsePrimitiveType(*type.primitive_type);
	} else if (type.struct_type) {
		auto &struct_type = *type.struct_type;
		child_list_t<LogicalType> struct_children;
		for (auto &field_p : struct_type.fields) {
			auto &child_field = *field_p;
			auto child = ParseStructField(child_field);
			struct_children.emplace_back(child->name, child->type);
			res->AddChild(std::move(child));
		}
		res->type = LogicalType::STRUCT(std::move(struct_children));
	} else if (type.list_type) {
		auto &list_type = *type.list_type;
		auto child_field =
		    CreateStructField("element", list_type.element_id, list_type.element_required, *list_type.element);
		auto child = ParseStructField(child_field);
		res->type = LogicalType::LIST(child->type);
		res->AddChild(std::move(child));
	} else if (type.map_type) {
		auto &map_type = *type.map_type;
		auto key_field = CreateStructField("key", map_type.key_id, true, *map_type.key);
		auto value_field = CreateStructField("value", map_type.value_id, map_type.value_required, *map_type.value);

		auto key = ParseStructField(key_field);
		auto value = ParseStructField(value_field);
		res->type = LogicalType::MAP(key->type, value->type);
		res->AddChild(std::move(key));
		res->AddChild(std::move(value));
	} else {
		throw InvalidConfigurationException("Encountered an invalid type in JSON schema");
	}

	if (field.initial_default) {
		res->initial_default = make_uniq<Value>(ParsePrimitiveValue(res->type, *field.initial_default));
	}
	if (field.write_default) {
		res->write_default = make_uniq<Value>(ParsePrimitiveValue(res->type, *field.write_default));
	}

	return res;
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
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::VARIANT:
	case LogicalTypeId::GEOMETRY:
		return true;
	default:
		return false;
	}
}

unique_ptr<IcebergColumnDefinition> IcebergColumnDefinition::Copy() const {
	auto res = make_uniq<IcebergColumnDefinition>();
	res->id = id;
	res->name = name;
	res->doc = doc;
	res->type = type;
	// TODO: initial default and write default need more support here
	if (initial_default) {
		res->initial_default = make_uniq<Value>(initial_default->Copy());
	}
	if (write_default) {
		res->write_default = make_uniq<Value>(write_default->Copy());
	}
	res->required = required;
	for (auto &child : children) {
		res->AddChild(child->Copy());
	}
	return res;
}

MultiFileColumnDefinition IcebergColumnDefinition::GetMultiFileColumnDefinition() const {
	MultiFileColumnDefinition column(name, type);
	if (!initial_default || initial_default->IsNull()) {
		if (type.id() == LogicalTypeId::STRUCT) {
			//! NOTE: spec defines {} as default value, but in practice no engine/catalog supports this
			vector<Value> child_values;
			for (auto &child : children) {
				auto child_column = child->GetMultiFileColumnDefinition();
				auto &child_default = child_column.default_expression->Cast<ConstantExpression>().GetValue();
				child_values.emplace_back(child_default);
			}
			auto default_value = Value::STRUCT(type, child_values);
			column.default_expression = make_uniq<ConstantExpression>(default_value);
		} else {
			column.default_expression = make_uniq<ConstantExpression>(Value(type));
		}
	} else {
		column.default_expression = make_uniq<ConstantExpression>(*initial_default);
	}
	column.identifier = Value::INTEGER(id);
	for (auto &child : children) {
		column.children.push_back(child->GetMultiFileColumnDefinition());
	}
	return column;
}

Value IcebergColumnDefinition::GetWriteDefault() const {
	optional_ptr<Value> default_to_use;
	if (write_default) {
		//! Use write-default if it's set
		default_to_use = write_default.get();
	} else if (initial_default) {
		//! If it's not set, use the initial-default (if that *is* set)
		default_to_use = initial_default.get();
	}
	auto res = ColumnDefinition(Identifier(name), type);
	if (default_to_use) {
		return *default_to_use;
	}
	return Value(type);
}

ColumnDefinition IcebergColumnDefinition::GetColumnDefinition() const {
	auto res = ColumnDefinition(Identifier(name), type);

	auto write_default = GetWriteDefault();
	if (!write_default.IsNull()) {
		if (type.IsNested()) {
			throw NotImplementedException("{} DEFAULT not supported for STRUCT yet");
		}
		res.SetDefaultValue(make_uniq<ConstantExpression>(write_default));
	} else if (type.id() == LogicalTypeId::STRUCT) {
		vector<Value> child_values;
		for (auto &child : children) {
			auto child_column = child->GetColumnDefinition();
			if (child_column.HasDefaultValue()) {
				auto &child_default = child_column.DefaultValue().Cast<ConstantExpression>();
				child_values.emplace_back(child_default.GetValue());
			} else {
				child_values.emplace_back(Value(child->type));
			}
		}
		auto default_value = Value::STRUCT(type, child_values);
		res.SetDefaultValue(make_uniq<ConstantExpression>(default_value));
	}

	if (doc) {
		//! Surface the Iceberg field `doc` as the DuckDB column comment
		res.SetComment(Value(*doc));
	}
	return res;
}

static bool DefaultsAreEqual(const unique_ptr<Value> &a, const unique_ptr<Value> &b) {
	const bool a_is_null = !a || a->IsNull();
	const bool b_is_null = !b || b->IsNull();

	if (a_is_null && b_is_null) {
		return true;
	}
	if (a_is_null != b_is_null) {
		return false;
	}
	return ValueOperations::NotDistinctFrom(*a, *b);
}

void IcebergColumnDefinition::RemoveChild(const string &name) {
	auto it = std::find_if(children.begin(), children.end(), [&name](const unique_ptr<IcebergColumnDefinition> &child) {
		return StringUtil::CIEquals(child->name, name);
	});
	if (it == children.end()) {
		throw InternalException("Can't delete child by name '%s', no child by that name exists", name);
	}
	children.erase(it);
	RewriteType();
}

void IcebergColumnDefinition::AddChild(unique_ptr<IcebergColumnDefinition> &&child) {
	child->parent = this;
	children.emplace_back(std::move(child));
	RewriteType();
}

idx_t IcebergColumnDefinition::GetChildCount() const {
	return children.size();
}

void IcebergColumnDefinition::RewriteType() {
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		child_list_t<LogicalType> struct_children;
		struct_children.reserve(children.size());
		for (auto &child : children) {
			struct_children.emplace_back(child->name, child->type);
		}
		type = LogicalType::STRUCT(std::move(struct_children));
		break;
	}
	case LogicalTypeId::LIST: {
		if (children.size() != 1) {
			return;
		}
		type = LogicalType::LIST(children[0]->type);
		break;
	}
	case LogicalTypeId::MAP: {
		if (children.size() != 2) {
			return;
		}
		type = LogicalType::MAP(children[0]->type, children[1]->type);
		break;
	}
	default:
		if (parent) {
			parent->RewriteType();
		}
		return;
	}
	if (parent) {
		parent->RewriteType();
	}
}

optional_ptr<const IcebergColumnDefinition> IcebergColumnDefinition::GetChild(const string &name) const {
	auto it = std::find_if(children.begin(), children.end(), [&name](const unique_ptr<IcebergColumnDefinition> &child) {
		return StringUtil::CIEquals(child->name, name);
	});
	if (it == children.end()) {
		return nullptr;
	}
	return (*it).get();
}

optional_ptr<const IcebergColumnDefinition> IcebergColumnDefinition::GetChild(idx_t index) const {
	if (index >= children.size()) {
		return nullptr;
	}
	return children[index].get();
}

const vector<unique_ptr<IcebergColumnDefinition>> &IcebergColumnDefinition::GetChildren() const {
	return children;
}

bool IcebergColumnDefinition::Equals(const IcebergColumnDefinition &other) const {
	if (id != other.id) {
		return false;
	}
	if (name != other.name) {
		return false;
	}
	if (type != other.type) {
		return false;
	}
	if (required != other.required) {
		return false;
	}
	if (doc != other.doc) {
		return false;
	}
	if (children.size() != other.children.size()) {
		return false;
	}
	for (idx_t i = 0; i < children.size(); i++) {
		auto &a_child = children[i];
		auto &b_child = other.children[i];
		if (!a_child->Equals(*b_child)) {
			return false;
		}
	}

	if (!DefaultsAreEqual(initial_default, other.initial_default)) {
		return false;
	}
	if (!DefaultsAreEqual(write_default, other.write_default)) {
		return false;
	}
	return true;
}

} // namespace duckdb
