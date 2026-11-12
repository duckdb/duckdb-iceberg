
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/primitive_type.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"

namespace duckdb {
namespace rest_api_objects {

class Literal {
public:
	Literal();
	Literal(const Literal &) = delete;
	Literal &operator=(const Literal &) = delete;
	Literal(Literal &&) = default;
	Literal &operator=(Literal &&) = default;
	class Object2 {
	public:
		Object2();
		Object2(const Object2 &) = delete;
		Object2 &operator=(const Object2 &) = delete;
		Object2(Object2 &&) = default;
		Object2 &operator=(Object2 &&) = default;

	public:
		// Deserialization
		static Object2 FromJSON(JSONValue obj);
		string TryFromJSON(JSONValue obj);

		// Copy
		Object2 Copy() const;

		// Serialization
		void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
		JSONMutableValue ToJSON(JSONWriter &writer) const;

	public:
		string type;
		PrimitiveTypeValue value;
		optional<PrimitiveType> data_type;
	};

public:
	// Deserialization
	static Literal FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	Literal Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<PrimitiveTypeValue> primitive_type_value;
	optional<Object2> object_2;
};

} // namespace rest_api_objects
} // namespace duckdb
