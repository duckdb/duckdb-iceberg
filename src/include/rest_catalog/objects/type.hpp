
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list_type.hpp"
#include "rest_catalog/objects/map_type.hpp"
#include "rest_catalog/objects/primitive_type.hpp"
#include "rest_catalog/objects/struct_type.hpp"
#include "rest_catalog/objects/variant_type.hpp"

namespace duckdb {
namespace rest_api_objects {

class Type {
public:
	Type();
	Type(const Type &) = delete;
	Type &operator=(const Type &) = delete;
	Type(Type &&) = default;
	Type &operator=(Type &&) = default;

public:
	// Deserialization
	static Type FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	Type Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<VariantType> variant_type;
	optional<PrimitiveType> primitive_type;
	optional<StructType> struct_type;
	optional<ListType> list_type;
	optional<MapType> map_type;
};

} // namespace rest_api_objects
} // namespace duckdb
