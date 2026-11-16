
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"

namespace duckdb {
namespace rest_api_objects {

class Type;

class StructField {
public:
	StructField();
	StructField(const StructField &) = delete;
	StructField &operator=(const StructField &) = delete;
	StructField(StructField &&) = default;
	StructField &operator=(StructField &&) = default;

public:
	// Deserialization
	static StructField FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	StructField Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	int32_t id;
	string name;
	unique_ptr<Type> type;
	bool required;
	optional<string> _doc;
	optional<PrimitiveTypeValue> initial_default;
	optional<PrimitiveTypeValue> write_default;
};

} // namespace rest_api_objects
} // namespace duckdb
