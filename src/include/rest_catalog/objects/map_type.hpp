
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class Type;

class MapType {
public:
	MapType();
	MapType(const MapType &) = delete;
	MapType &operator=(const MapType &) = delete;
	MapType(MapType &&) = default;
	MapType &operator=(MapType &&) = default;

public:
	// Deserialization
	static MapType FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	MapType Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	int32_t key_id;
	unique_ptr<Type> key;
	int32_t value_id;
	unique_ptr<Type> value;
	bool value_required;
};

} // namespace rest_api_objects
} // namespace duckdb
