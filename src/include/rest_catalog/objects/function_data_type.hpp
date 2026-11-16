
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/function_list_type.hpp"
#include "rest_catalog/objects/function_map_type.hpp"
#include "rest_catalog/objects/function_struct_type.hpp"

namespace duckdb {
namespace rest_api_objects {

class FunctionDataType {
public:
	FunctionDataType();
	FunctionDataType(const FunctionDataType &) = delete;
	FunctionDataType &operator=(const FunctionDataType &) = delete;
	FunctionDataType(FunctionDataType &&) = default;
	FunctionDataType &operator=(FunctionDataType &&) = default;
	class FunctionDataTypeOneOf1 {
	public:
		FunctionDataTypeOneOf1();
		FunctionDataTypeOneOf1(const FunctionDataTypeOneOf1 &) = delete;
		FunctionDataTypeOneOf1 &operator=(const FunctionDataTypeOneOf1 &) = delete;
		FunctionDataTypeOneOf1(FunctionDataTypeOneOf1 &&) = default;
		FunctionDataTypeOneOf1 &operator=(FunctionDataTypeOneOf1 &&) = default;

	public:
		// Deserialization
		static FunctionDataTypeOneOf1 FromJSON(JSONValue obj);
		string TryFromJSON(JSONValue obj);

		// Copy
		FunctionDataTypeOneOf1 Copy() const;

		// Serialization
		JSONMutableValue ToJSON(JSONWriter &writer) const;

	public:
		string value;
	};

public:
	// Deserialization
	static FunctionDataType FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FunctionDataType Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<FunctionDataTypeOneOf1> function_data_type_one_of_1;
	optional<FunctionListType> function_list_type;
	optional<FunctionMapType> function_map_type;
	optional<FunctionStructType> function_struct_type;
};

} // namespace rest_api_objects
} // namespace duckdb
