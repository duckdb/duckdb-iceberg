#pragma once

#include "rest_catalog/objects/struct_field.hpp"
#include "rest_catalog/objects/primitive_type.hpp"
#include "rest_catalog/objects/type.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

struct IcebergColumnDefinition {
public:
	static unique_ptr<IcebergColumnDefinition> ParseStructField(rest_api_objects::StructField &field);

public:
	static LogicalType ParsePrimitiveType(rest_api_objects::PrimitiveType &type);
	static LogicalType ParsePrimitiveTypeString(const string &type_str);
	static unique_ptr<IcebergColumnDefinition>
	ParseType(const string &name, int32_t field_id, bool required, rest_api_objects::Type &iceberg_type,
	          optional_ptr<rest_api_objects::PrimitiveTypeValue> initial_default = nullptr);
	bool IsPrimitiveType();

public:
	int32_t id;
	string name;
	LogicalType type;
	Value initial_default;
	bool required;
	vector<unique_ptr<IcebergColumnDefinition>> children;
};

} // namespace duckdb
