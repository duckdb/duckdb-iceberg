#pragma once

#include "rest_catalog/objects/struct_field.hpp"
#include "rest_catalog/objects/primitive_type.hpp"
#include "rest_catalog/objects/type.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

struct IcebergColumnDefinition {
public:
	static unique_ptr<IcebergColumnDefinition> ParseStructField(rest_api_objects::StructField &field);

public:
	static LogicalType ParsePrimitiveType(rest_api_objects::PrimitiveType &type);
	static LogicalType ParsePrimitiveTypeString(const string &type_str);
	static unique_ptr<IcebergColumnDefinition>
	ParseType(const string &name, int32_t field_id, bool required, rest_api_objects::Type &iceberg_type,
	          optional_ptr<rest_api_objects::PrimitiveTypeValue> initial_default = nullptr,
	          optional_ptr<rest_api_objects::PrimitiveTypeValue> write_default = nullptr);
	bool IsIcebergPrimitiveType() const;
	ColumnDefinition GetColumnDefinition() const;
	unique_ptr<IcebergColumnDefinition> Copy() const;

public:
	int32_t id;
	string name;
	LogicalType type;
	unique_ptr<Value> initial_default;
	unique_ptr<Value> write_default;
	bool required;
	vector<unique_ptr<IcebergColumnDefinition>> children;
};

} // namespace duckdb
