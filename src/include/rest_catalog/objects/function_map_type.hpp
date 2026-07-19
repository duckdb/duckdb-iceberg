
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FunctionDataType;

class FunctionMapType {
public:
	FunctionMapType();
	FunctionMapType(const FunctionMapType &) = delete;
	FunctionMapType &operator=(const FunctionMapType &) = delete;
	FunctionMapType(FunctionMapType &&) = default;
	FunctionMapType &operator=(FunctionMapType &&) = default;

public:
	// Deserialization
	static FunctionMapType FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	FunctionMapType Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string type;
	unique_ptr<FunctionDataType> key;
	unique_ptr<FunctionDataType> value;
};

} // namespace rest_api_objects
} // namespace duckdb
