
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/expression_type.hpp"

namespace duckdb {
namespace rest_api_objects {

class FalseExpression {
public:
	FalseExpression();
	FalseExpression(const FalseExpression &) = delete;
	FalseExpression &operator=(const FalseExpression &) = delete;
	FalseExpression(FalseExpression &&) = default;
	FalseExpression &operator=(FalseExpression &&) = default;

public:
	// Deserialization
	static FalseExpression FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FalseExpression Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ExpressionType type;
};

} // namespace rest_api_objects
} // namespace duckdb
