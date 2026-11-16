
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/integer_type_value.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"

namespace duckdb {
namespace rest_api_objects {

class ValueMap {
public:
	ValueMap();
	ValueMap(const ValueMap &) = delete;
	ValueMap &operator=(const ValueMap &) = delete;
	ValueMap(ValueMap &&) = default;
	ValueMap &operator=(ValueMap &&) = default;

public:
	// Deserialization
	static ValueMap FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ValueMap Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<vector<IntegerTypeValue>> keys;
	optional<vector<PrimitiveTypeValue>> values;
};

} // namespace rest_api_objects
} // namespace duckdb
