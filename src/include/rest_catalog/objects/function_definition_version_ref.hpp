
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FunctionDefinitionVersionRef {
public:
	FunctionDefinitionVersionRef();
	FunctionDefinitionVersionRef(const FunctionDefinitionVersionRef &) = delete;
	FunctionDefinitionVersionRef &operator=(const FunctionDefinitionVersionRef &) = delete;
	FunctionDefinitionVersionRef(FunctionDefinitionVersionRef &&) = default;
	FunctionDefinitionVersionRef &operator=(FunctionDefinitionVersionRef &&) = default;

public:
	// Deserialization
	static FunctionDefinitionVersionRef FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	FunctionDefinitionVersionRef Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string definition_id;
	int32_t version_id;
};

} // namespace rest_api_objects
} // namespace duckdb
