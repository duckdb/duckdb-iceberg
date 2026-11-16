
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

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
	static FunctionDefinitionVersionRef FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FunctionDefinitionVersionRef Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string definition_id;
	int32_t version_id;
};

} // namespace rest_api_objects
} // namespace duckdb
