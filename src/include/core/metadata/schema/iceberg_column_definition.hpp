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
	void SetWriteDefault(const Value &default_value);

public:
	void AddChild(unique_ptr<IcebergColumnDefinition> &&child);
	void RemoveChild(const string &name);
	optional_ptr<const IcebergColumnDefinition> GetChild(const string &name) const;
	optional_ptr<const IcebergColumnDefinition> GetChild(idx_t index) const;
	const vector<unique_ptr<IcebergColumnDefinition>> &GetChildren() const;
	idx_t GetChildCount() const;
	void RewriteType();

private:
	Value GetInitialDefault() const;
	Value GetWriteDefault() const;
	//! Returns an internal, recursive description of a STRUCT write default.
	//!
	//! `struct_default` is the default for the whole STRUCT. `field_defaults` contains the defaults used only after
	//! a non-NULL STRUCT value has been supplied. Keeping these values separate is required by Iceberg: a missing
	//! nested STRUCT field uses its whole-STRUCT default, while a present nested STRUCT has its omitted children
	//! filled recursively.
	//!
	//! This descriptor is carried through DuckDB's default-expression plumbing as the second argument of a
	//! `constant_or_null` envelope. It is internal metadata and is never serialized to Iceberg.
	Value GetWriteDefaultDescriptor() const;

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
