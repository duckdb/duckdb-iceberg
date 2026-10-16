
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/catalog_object_identifier.hpp"
#include "rest_catalog/objects/function_argument.hpp"

namespace duckdb {
namespace rest_api_objects {

class Apply {
public:
	Apply();
	Apply(const Apply &) = delete;
	Apply &operator=(const Apply &) = delete;
	Apply(Apply &&) = default;
	Apply &operator=(Apply &&) = default;
	class Object4 {
	public:
		Object4();
		Object4(const Object4 &) = delete;
		Object4 &operator=(const Object4 &) = delete;
		Object4(Object4 &&) = default;
		Object4 &operator=(Object4 &&) = default;
		class InlineSchema2 {
		public:
			InlineSchema2();
			InlineSchema2(const InlineSchema2 &) = delete;
			InlineSchema2 &operator=(const InlineSchema2 &) = delete;
			InlineSchema2(InlineSchema2 &&) = default;
			InlineSchema2 &operator=(InlineSchema2 &&) = default;

		public:
			// Deserialization
			static InlineSchema2 FromJSON(JSONValue obj);
			string TryFromJSON(JSONValue obj);

			// Copy
			InlineSchema2 Copy() const;

			// Serialization
			JSONMutableValue ToJSON(JSONWriter &writer) const;

		public:
			string value;
		};

		class Object3 {
		public:
			Object3();
			Object3(const Object3 &) = delete;
			Object3 &operator=(const Object3 &) = delete;
			Object3(Object3 &&) = default;
			Object3 &operator=(Object3 &&) = default;

		public:
			// Deserialization
			static Object3 FromJSON(JSONValue obj);
			string TryFromJSON(JSONValue obj);

			// Copy
			Object3 Copy() const;

			// Serialization
			void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
			JSONMutableValue ToJSON(JSONWriter &writer) const;

		public:
			CatalogObjectIdentifier identifier;
			optional<string> catalog;
		};

	public:
		// Deserialization
		static Object4 FromJSON(JSONValue obj);
		string TryFromJSON(JSONValue obj);

		// Copy
		Object4 Copy() const;

		// Serialization
		JSONMutableValue ToJSON(JSONWriter &writer) const;

	public:
		optional<InlineSchema2> inline_schema_2;
		optional<CatalogObjectIdentifier> catalog_object_identifier;
		optional<Object3> object_3;
	};

public:
	// Deserialization
	static Apply FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	Apply Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	Object4 function;
	vector<FunctionArgument> arguments;
};

} // namespace rest_api_objects
} // namespace duckdb
