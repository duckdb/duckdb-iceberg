
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/literal.hpp"
#include "rest_catalog/objects/primitive_type.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"

namespace duckdb {
namespace rest_api_objects {

class Literals {
public:
	Literals();
	Literals(const Literals &) = delete;
	Literals &operator=(const Literals &) = delete;
	Literals(Literals &&) = default;
	Literals &operator=(Literals &&) = default;
	class Object5 {
	public:
		Object5();
		Object5(const Object5 &) = delete;
		Object5 &operator=(const Object5 &) = delete;
		Object5(Object5 &&) = default;
		Object5 &operator=(Object5 &&) = default;

	public:
		// Deserialization
		static Object5 FromJSON(JSONValue obj);
		string TryFromJSON(JSONValue obj);

		// Copy
		Object5 Copy() const;

		// Serialization
		void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
		JSONMutableValue ToJSON(JSONWriter &writer) const;

	public:
		string type;
		vector<PrimitiveTypeValue> values;
		PrimitiveType data_type;
	};

	class LiteralsOneOf1 {
	public:
		LiteralsOneOf1();
		LiteralsOneOf1(const LiteralsOneOf1 &) = delete;
		LiteralsOneOf1 &operator=(const LiteralsOneOf1 &) = delete;
		LiteralsOneOf1(LiteralsOneOf1 &&) = default;
		LiteralsOneOf1 &operator=(LiteralsOneOf1 &&) = default;

	public:
		// Deserialization
		static LiteralsOneOf1 FromJSON(JSONValue obj);
		string TryFromJSON(JSONValue obj);

		// Copy
		LiteralsOneOf1 Copy() const;

		// Serialization
		JSONMutableValue ToJSON(JSONWriter &writer) const;

	public:
		vector<Literal> value;
	};

public:
	// Deserialization
	static Literals FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	Literals Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<LiteralsOneOf1> literals_one_of_1;
	optional<Object5> object_5;
};

} // namespace rest_api_objects
} // namespace duckdb
