
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/function_sqlrepresentation.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FunctionRepresentation {
public:
	FunctionRepresentation();
	FunctionRepresentation(const FunctionRepresentation &) = delete;
	FunctionRepresentation &operator=(const FunctionRepresentation &) = delete;
	FunctionRepresentation(FunctionRepresentation &&) = default;
	FunctionRepresentation &operator=(FunctionRepresentation &&) = default;

public:
	// Deserialization
	static FunctionRepresentation FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	FunctionRepresentation Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	optional<FunctionSQLRepresentation> function_sqlrepresentation;
};

} // namespace rest_api_objects
} // namespace duckdb
