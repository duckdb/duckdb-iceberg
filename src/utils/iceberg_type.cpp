#include "duckdb/common/string_util.hpp"
#include "utils/iceberg_type.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "rest_catalog/objects/list_type.hpp"
#include "rest_catalog/objects/map_type.hpp"
#include "rest_catalog/objects/struct_type.hpp"
#include "rest_catalog/objects/struct_field.hpp"
#include "rest_catalog/objects/type.hpp"

namespace duckdb {

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
	case LogicalTypeId::MAP:
		return "map";
	default:
		throw InvalidInputException("Column type %s is not a valid Iceberg Type.", LogicalTypeIdToString(type.id()));
	}
}

rest_api_objects::Type IcebergTypeHelper::CreateIcebergRestType(const LogicalType &type,
                                                                std::function<idx_t()> get_next_id) {
	rest_api_objects::Type rest_type;

	switch (type.id()) {
	case LogicalTypeId::MAP: {
		rest_type.has_primitive_type = false;
		rest_type.has_map_type = true;
		rest_type.map_type = rest_api_objects::MapType();
		auto key_type = MapType::KeyType(type);
		auto value_type = MapType::ValueType(type);
		rest_type.map_type.key_id = static_cast<int32_t>(get_next_id());
		rest_type.map_type.key =
		    make_uniq<rest_api_objects::Type>(IcebergTypeHelper::CreateIcebergRestType(key_type, get_next_id));
		rest_type.map_type.value_id = static_cast<int32_t>(get_next_id());
		rest_type.map_type.value =
		    make_uniq<rest_api_objects::Type>(IcebergTypeHelper::CreateIcebergRestType(value_type, get_next_id));
		rest_type.map_type.value_required = false;
		return rest_type;
	}
	case LogicalTypeId::STRUCT: {
		rest_type.has_primitive_type = false;
		rest_type.has_struct_type = true;
		rest_type.struct_type = rest_api_objects::StructType();
		auto &children = StructType::GetChildTypes(type);
		for (auto &child : children) {
			auto struct_child = make_uniq<rest_api_objects::StructField>();
			struct_child->name = child.first;
			struct_child->id = get_next_id();
			struct_child->type =
			    make_uniq<rest_api_objects::Type>(IcebergTypeHelper::CreateIcebergRestType(child.second, get_next_id));
			struct_child->has_doc = false;
			struct_child->required = false;
			struct_child->has_initial_default = false;
			rest_type.struct_type.fields.push_back(std::move(struct_child));
		}
		return rest_type;
	}
	case LogicalTypeId::LIST: {
		rest_type.has_primitive_type = false;
		rest_type.has_list_type = true;
		rest_type.list_type = rest_api_objects::ListType();
		auto list_child_type = ListType::GetChildType(type);
		rest_type.list_type.type = IcebergTypeHelper::LogicalTypeToIcebergType(list_child_type);
		rest_type.list_type.element_id = get_next_id();
		rest_type.list_type.element =
		    make_uniq<rest_api_objects::Type>(IcebergTypeHelper::CreateIcebergRestType(list_child_type, get_next_id));
		rest_type.list_type.element_required = false;
		return rest_type;
	}
	case LogicalTypeId::ARRAY: {
		throw InvalidConfigurationException("Array type not supported in Iceberg type. Please cast to LIST");
	}
	default:
		break;
	}
	rest_type.has_primitive_type = true;
	rest_type.primitive_type = rest_api_objects::PrimitiveType();
	rest_type.primitive_type.value = IcebergTypeHelper::LogicalTypeToIcebergType(type);
	return rest_type;
}

} // namespace duckdb
