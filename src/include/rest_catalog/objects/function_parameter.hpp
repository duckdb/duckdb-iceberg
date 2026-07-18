
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

class FunctionParameter {
public:
	FunctionParameter();
	FunctionParameter(const FunctionParameter &) = delete;
	FunctionParameter &operator=(const FunctionParameter &) = delete;
	FunctionParameter(FunctionParameter &&) = default;
	FunctionParameter &operator=(FunctionParameter &&) = default;

public:
	// Deserialization
	static FunctionParameter FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	FunctionParameter Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	unique_ptr<FunctionDataType> type;
	string name;
	optional<string> _doc;
};

} // namespace rest_api_objects
} // namespace duckdb
