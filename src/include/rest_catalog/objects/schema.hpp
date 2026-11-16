
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/struct_type.hpp"

namespace duckdb {
namespace rest_api_objects {

class Schema {
public:
	Schema();
	Schema(const Schema &) = delete;
	Schema &operator=(const Schema &) = delete;
	Schema(Schema &&) = default;
	Schema &operator=(Schema &&) = default;
	class Object1 {
	public:
		Object1();
		Object1(const Object1 &) = delete;
		Object1 &operator=(const Object1 &) = delete;
		Object1(Object1 &&) = default;
		Object1 &operator=(Object1 &&) = default;

	public:
		// Deserialization
		static Object1 FromJSON(JSONValue obj);
		string TryFromJSON(JSONValue obj);

		// Copy
		Object1 Copy() const;

		// Serialization
		void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
		JSONMutableValue ToJSON(JSONWriter &writer) const;

	public:
		optional<int32_t> schema_id;
		optional<vector<int32_t>> identifier_field_ids;
	};

public:
	// Deserialization
	static Schema FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	Schema Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	StructType struct_type;
	Object1 object_1;
};

} // namespace rest_api_objects
} // namespace duckdb
