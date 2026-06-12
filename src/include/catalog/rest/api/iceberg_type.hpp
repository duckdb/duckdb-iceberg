
#pragma once

#include "rest_catalog/objects/type.hpp"
#include "yyjson.hpp"
#include <functional>
#include "rest_catalog/objects/primitive_type_value.hpp"

namespace duckdb {

class IcebergTypeHelper {
public:
	static rest_api_objects::StructField CreateIcebergRestType(const string &name, const LogicalType &type,
	                                                           bool required, const string &doc,
	                                                           const Value &default_value,
	                                                           const std::function<idx_t()> &get_next_id);
	static string LogicalTypeToIcebergType(const LogicalType &type);
	static rest_api_objects::PrimitiveTypeValue PrimitiveTypeFromValue(const Value &val);
	static yyjson_mut_val *PrimitiveTypeValueToJSON(yyjson_mut_doc *doc,
	                                                const rest_api_objects::PrimitiveTypeValue &value);
};

} // namespace duckdb
