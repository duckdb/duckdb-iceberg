#include "catalog/rest/api/iceberg_type.hpp"
#include "common/iceberg_constants.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/parser/column_definition.hpp"

#include "rest_catalog/objects/list_type.hpp"
#include "rest_catalog/objects/map_type.hpp"
#include "rest_catalog/objects/struct_type.hpp"
#include "rest_catalog/objects/struct_field.hpp"
#include "rest_catalog/objects/type.hpp"

namespace duckdb {

static string ConvertBlobDefault(const string_t &str) {
	string result;
	result.resize(str.GetSize() * 2);
	idx_t str_idx = 0;
	auto data = str.GetData();
	auto len = str.GetSize();
	for (idx_t i = 0; i < len; i++) {
		auto byte_a = (data[i] >> 4) & 0x0F;
		auto byte_b = data[i] & 0x0F;
		D_ASSERT(byte_a >= 0 && byte_a < 16);
		D_ASSERT(byte_b >= 0 && byte_b < 16);
		// non-ascii characters are rendered as hexadecimal (e.g. \x00)
		result[str_idx++] = Blob::HEX_TABLE[byte_a];
		result[str_idx++] = Blob::HEX_TABLE[byte_b];
	}
	return result;
}

string IcebergTypeHelper::LogicalTypeToIcebergType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::INTEGER:
		return "int";
	case LogicalTypeId::BOOLEAN:
		return "boolean";
	case LogicalTypeId::VARCHAR:
		return "string";
	case LogicalTypeId::DATE:
		return "date";
	case LogicalTypeId::BIGINT:
		return "long";
	case LogicalTypeId::HUGEINT:
		// Iceberg doesnt have native 128bit int, decimal(38,0) covers the range
		return "decimal(38, 0)";
	case LogicalTypeId::FLOAT:
		return "float";
	case LogicalTypeId::DOUBLE:
		return "double";
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		return StringUtil::Format("decimal(%d, %d)", width, scale);
	}
	case LogicalTypeId::UUID:
		return "uuid";
	case LogicalTypeId::BLOB:
		return "binary";
	case LogicalTypeId::STRUCT:
		return "struct";
	case LogicalTypeId::LIST:
		// Iceberg doesn't support fixed array lengths
		return "list";
	case LogicalTypeId::TIME:
		return "time";
	case LogicalTypeId::TIMESTAMP:
		return "timestamp";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "timestamptz";
	case LogicalTypeId::TIMESTAMP_NS:
		return "timestamp_ns";
	case LogicalTypeId::MAP:
		return "map";
	case LogicalTypeId::VARIANT:
		return "variant";
	case LogicalTypeId::GEOMETRY: {
		if (GeoType::HasCRS(type)) {
			return StringUtil::Format("geometry(%s)", GeoType::GetCRS(type).GetIdentifier());
		}
		// use default coordinate system
		return "geometry(" + StringUtil::Lower(IcebergConstants::DefaultGeometryCRS) + ")";
	}
	default:
		throw InvalidInputException("Column type %s is not a valid Iceberg Type.", LogicalTypeIdToString(type.id()));
	}
}

yyjson_mut_val *IcebergTypeHelper::PrimitiveTypeValueToJSON(yyjson_mut_doc *doc,
                                                            const rest_api_objects::PrimitiveTypeValue &value) {
	if (value.has_boolean_type_value) {
		return yyjson_mut_bool(doc, value.boolean_type_value.value);
	} else if (value.has_long_type_value) {
		return yyjson_mut_int(doc, value.long_type_value.value);
	} else if (value.has_integer_type_value) {
		return yyjson_mut_int(doc, value.integer_type_value.value);
	} else if (value.has_double_type_value) {
		return yyjson_mut_real(doc, value.double_type_value.value);
	} else if (value.has_float_type_value) {
		return yyjson_mut_real(doc, value.float_type_value.value);
	} else if (value.has_decimal_type_value) {
		auto &str = value.decimal_type_value.value;
		return yyjson_mut_strncpy(doc, str.c_str(), str.size());
	} else if (value.has_string_type_value) {
		auto &str = value.string_type_value.value;
		return yyjson_mut_strncpy(doc, str.c_str(), str.size());
	} else if (value.has_uuidtype_value) {
		auto &str = value.uuidtype_value.value;
		return yyjson_mut_strncpy(doc, str.c_str(), str.size());
	} else if (value.has_date_type_value) {
		auto &str = value.date_type_value.value;
		return yyjson_mut_strncpy(doc, str.c_str(), str.size());
	} else if (value.has_time_type_value) {
		auto &str = value.time_type_value.value;
		return yyjson_mut_strncpy(doc, str.c_str(), str.size());
	} else if (value.has_timestamp_type_value) {
		auto &str = value.timestamp_type_value.value;
		return yyjson_mut_strncpy(doc, str.c_str(), str.size());
	} else if (value.has_timestamp_tz_type_value) {
		auto &str = value.timestamp_tz_type_value.value;
		return yyjson_mut_strncpy(doc, str.c_str(), str.size());
	} else if (value.has_timestamp_nano_type_value) {
		auto &str = value.timestamp_nano_type_value.value;
		return yyjson_mut_strncpy(doc, str.c_str(), str.size());
	} else if (value.has_timestamp_tz_nano_type_value) {
		auto &str = value.timestamp_tz_nano_type_value.value;
		return yyjson_mut_strncpy(doc, str.c_str(), str.size());
	} else if (value.has_fixed_type_value) {
		auto &str = value.fixed_type_value.value;
		return yyjson_mut_strncpy(doc, str.c_str(), str.size());
	} else if (value.has_binary_type_value) {
		auto &str = value.binary_type_value.value;
		return yyjson_mut_strncpy(doc, str.c_str(), str.size());
	} else {
		return yyjson_mut_null(doc);
	}
}

rest_api_objects::PrimitiveTypeValue IcebergTypeHelper::PrimitiveTypeFromValue(const Value &value) {
	rest_api_objects::PrimitiveTypeValue result;

	if (value.IsNull()) {
		throw InternalException("Can't produce a PrimitiveTypeValue from NULL");
	}
	auto &type = value.type();
	switch (type.id()) {
	case LogicalTypeId::VARIANT: {
		throw NotImplementedException("DEFAULT values for VARIANT are not supported yet");
	}
	//! BooleanTypeValue
	case LogicalTypeId::BOOLEAN: {
		result.boolean_type_value.value = value.GetValue<bool>();
		result.has_boolean_type_value = true;
		return result;
	}
	//! IntegerTypeValue
	case LogicalTypeId::INTEGER: {
		result.integer_type_value.value = value.GetValue<int32_t>();
		result.has_integer_type_value = true;
		return result;
	}
	//! LongTypeValue
	case LogicalTypeId::BIGINT: {
		result.long_type_value.value = value.GetValue<int64_t>();
		result.has_long_type_value = true;
		return result;
	}
	//! FloatTypeValue
	case LogicalTypeId::FLOAT: {
		result.float_type_value.value = value.GetValue<float>();
		result.has_float_type_value = true;
		return result;
	}
	//! DoubleTypeValue
	case LogicalTypeId::DOUBLE: {
		result.double_type_value.value = value.GetValue<double>();
		result.has_double_type_value = true;
		return result;
	}
	//! DecimalTypeValue
	case LogicalTypeId::DECIMAL: {
		//! FIXME: Spec says scientific notation should be used for negative scale decimals
		result.string_type_value.value = value.ToString();
		result.has_string_type_value = true;
		return result;
	}
	//! NOTE: when parsing we can't differentiate between these, so we set string_type
	//! StringTypeValue
	//! UUIDTypeValue
	//! DateTypeValue
	//! TimeTypeValue
	case LogicalTypeId::UUID:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::VARCHAR: {
		result.string_type_value.value = value.ToString();
		result.has_string_type_value = true;
		return result;
	}
	//! TimestampTypeValue
	//! TimestampNanoTypeValue
	//! TimestampTzTypeValue
	//! TimestampTzNanoTypeValue
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS: {
		auto raw = value.ToString();
		auto splits = StringUtil::Split(raw, ' ');
		D_ASSERT(splits.size() == 2);
		auto str = StringUtil::Join(splits, "T");

		if (type.id() == LogicalTypeId::TIMESTAMP || type.id() == LogicalTypeId::TIMESTAMP_NS) {
			result.string_type_value.value = str;
			result.has_string_type_value = true;
			return result;
		}

		str += ":00";
		result.string_type_value.value = str;
		result.has_string_type_value = true;
	}
	//! FIXME: missing FixedTypeValue
	//! BinaryTypeValue
	case LogicalTypeId::BLOB: {
		auto str = value.GetValueUnsafe<string_t>();
		auto blob_str = ConvertBlobDefault(str);

		result.binary_type_value.value = blob_str;
		result.has_binary_type_value = true;
		return result;
	}
	default:
		throw InvalidConfigurationException("Type %s not supported for Iceberg tables", type.ToString());
	}
}

rest_api_objects::StructField IcebergTypeHelper::CreateIcebergRestType(const string &name, const LogicalType &type,
                                                                       bool required, const string &doc,
                                                                       const Value &default_val,
                                                                       const std::function<idx_t()> &get_next_id) {
	rest_api_objects::StructField result;
	result.id = static_cast<int32_t>(get_next_id());
	result.name = name;
	result.type = make_uniq<rest_api_objects::Type>();
	result.required = required;
	if (!doc.empty()) {
		result.has_doc = true;
		result.doc = doc;
	}
	if (!default_val.IsNull()) {
		if (type.IsNested() && type.id() != LogicalTypeId::VARIANT) {
			throw NotImplementedException("DEFAULT values for nested types (like %s) not implemented", type.ToString());
		}
		result.has_initial_default = true;
		result.initial_default = std::move(IcebergTypeHelper::PrimitiveTypeFromValue(default_val));
	}
	auto &rest_type = *result.type;

	switch (type.id()) {
	case LogicalTypeId::MAP: {
		rest_type.has_map_type = true;
		rest_type.map_type = rest_api_objects::MapType();

		//! Key
		auto key_type = MapType::KeyType(type);
		auto key_field = IcebergTypeHelper::CreateIcebergRestType("key", key_type, true, "",
		                                                          Value(), //! FIXME: extract from parent
		                                                          get_next_id);
		rest_type.map_type.key_id = key_field.id;
		rest_type.map_type.key = std::move(key_field.type);

		//! Value
		auto value_type = MapType::ValueType(type);
		auto value_field = IcebergTypeHelper::CreateIcebergRestType("value", value_type, false, "",
		                                                            Value(), //! FIXME: extract from parent
		                                                            get_next_id);
		rest_type.map_type.value_id = value_field.id;
		rest_type.map_type.value = std::move(value_field.type);
		rest_type.map_type.value_required = value_field.required;

		return result;
	}
	case LogicalTypeId::STRUCT: {
		rest_type.has_struct_type = true;
		rest_type.struct_type = rest_api_objects::StructType();
		auto &children = StructType::GetChildTypes(type);
		for (auto &child : children) {
			auto &child_name = child.first;
			auto &child_type = child.second;
			auto struct_child = make_uniq<rest_api_objects::StructField>(
			    IcebergTypeHelper::CreateIcebergRestType(child_name, child_type, false, "",
			                                             Value(), //! FIXME: extract the default value from the parent
			                                             get_next_id));
			rest_type.struct_type.fields.push_back(std::move(struct_child));
		}
		return result;
	}
	case LogicalTypeId::LIST: {
		rest_type.has_list_type = true;
		rest_type.list_type = rest_api_objects::ListType();

		//! Element
		const auto &element_type = ListType::GetChildType(type);
		auto element_field = IcebergTypeHelper::CreateIcebergRestType("element", element_type, false, "",
		                                                              Value(), //! FIXME: extract default from parent
		                                                              get_next_id);
		rest_type.list_type.type = "list";
		rest_type.list_type.element_id = element_field.id;
		rest_type.list_type.element = std::move(element_field.type);
		rest_type.list_type.element_required = element_field.required;
		return result;
	}
	case LogicalTypeId::ARRAY: {
		throw InvalidConfigurationException("Array type not supported in Iceberg type. Please cast to LIST");
	}
	default:
		rest_type.has_primitive_type = true;
		rest_type.primitive_type = rest_api_objects::PrimitiveType();
		rest_type.primitive_type.value = IcebergTypeHelper::LogicalTypeToIcebergType(type);
		return result;
	}
}

} // namespace duckdb
