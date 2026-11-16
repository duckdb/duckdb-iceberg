
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/expression_type.hpp"

namespace duckdb {
namespace rest_api_objects {

class TrueExpression {
public:
	TrueExpression();
	TrueExpression(const TrueExpression &) = delete;
	TrueExpression &operator=(const TrueExpression &) = delete;
	TrueExpression(TrueExpression &&) = default;
	TrueExpression &operator=(TrueExpression &&) = default;

public:
	// Deserialization
	static TrueExpression FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	TrueExpression Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ExpressionType type;
};

} // namespace rest_api_objects
} // namespace duckdb
