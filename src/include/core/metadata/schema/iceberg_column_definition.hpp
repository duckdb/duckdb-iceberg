#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

#include "rest_catalog/objects/struct_field.hpp"
#include "rest_catalog/objects/primitive_type.hpp"
#include "rest_catalog/objects/type.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"

namespace duckdb {

struct IcebergColumnDefinition {
public:
	static unique_ptr<IcebergColumnDefinition> ParseStructField(const rest_api_objects::StructField &field);

public:
	static LogicalType ParsePrimitiveType(const rest_api_objects::PrimitiveType &type);
	static LogicalType ParsePrimitiveTypeString(const string &type_str);
	static Value ParsePrimitiveValue(const LogicalType &type,
	                                 const rest_api_objects::PrimitiveTypeValue &primitive_value);
	bool IsIcebergPrimitiveType() const;

	ColumnDefinition GetColumnDefinition() const;
	MultiFileColumnDefinition GetMultiFileColumnDefinition() const;
	unique_ptr<IcebergColumnDefinition> Copy() const;
	bool Equals(const IcebergColumnDefinition &other) const;

public:
	void AddChild(unique_ptr<IcebergColumnDefinition> &&child);
	void RemoveChild(const string &name);
	optional_ptr<const IcebergColumnDefinition> GetChild(const string &name) const;
	optional_ptr<const IcebergColumnDefinition> GetChild(idx_t index) const;
	const vector<unique_ptr<IcebergColumnDefinition>> &GetChildren() const;
	idx_t GetChildCount() const;
	void RewriteType();

private:
	Value GetWriteDefault() const;

public:
	int32_t id;
	optional<string> doc;
	string name;
	LogicalType type;
	unique_ptr<Value> initial_default;
	unique_ptr<Value> write_default;
	bool required;

private:
	optional_ptr<IcebergColumnDefinition> parent;
	vector<unique_ptr<IcebergColumnDefinition>> children;
};

} // namespace duckdb
