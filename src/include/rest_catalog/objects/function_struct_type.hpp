
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/function_struct_field.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FunctionStructType {
public:
	FunctionStructType();
	FunctionStructType(const FunctionStructType &) = delete;
	FunctionStructType &operator=(const FunctionStructType &) = delete;
	FunctionStructType(FunctionStructType &&) = default;
	FunctionStructType &operator=(FunctionStructType &&) = default;

public:
	// Deserialization
	static FunctionStructType FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	FunctionStructType Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string type;
	vector<FunctionStructField> fields;
};

} // namespace rest_api_objects
} // namespace duckdb
