
#pragma once

#include "rest_catalog/objects/type.hpp"
#include "duckdb/common/json_document.hpp"
#include <functional>
#include "rest_catalog/objects/primitive_type_value.hpp"

namespace duckdb {

class IcebergTypeHelper {
public:
	static rest_api_objects::StructField
	CreateIcebergRestType(const string &name, const LogicalType &type, bool required, const string &doc,
	                      const Value &default_value, const std::function<idx_t()> &get_next_id, idx_t iceberg_version);
	static string LogicalTypeToIcebergType(const LogicalType &type);
	//! The lowest format version whose spec allows this type, including any nested types
	static int32_t MinimumFormatVersion(const LogicalType &type);
	static rest_api_objects::PrimitiveTypeValue PrimitiveTypeFromValue(const Value &val);
	static JSONMutableValue PrimitiveTypeValueToJSON(JSONWriter &writer,
	                                                 const rest_api_objects::PrimitiveTypeValue &value);
};

} // namespace duckdb
