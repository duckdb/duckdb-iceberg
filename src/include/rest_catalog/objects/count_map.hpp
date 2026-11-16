
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/integer_type_value.hpp"
#include "rest_catalog/objects/long_type_value.hpp"

namespace duckdb {
namespace rest_api_objects {

class CountMap {
public:
	CountMap();
	CountMap(const CountMap &) = delete;
	CountMap &operator=(const CountMap &) = delete;
	CountMap(CountMap &&) = default;
	CountMap &operator=(CountMap &&) = default;

public:
	// Deserialization
	static CountMap FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	CountMap Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<vector<IntegerTypeValue>> keys;
	optional<vector<LongTypeValue>> values;
};

} // namespace rest_api_objects
} // namespace duckdb
