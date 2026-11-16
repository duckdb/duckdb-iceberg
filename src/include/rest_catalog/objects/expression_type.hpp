
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class ExpressionType {
public:
	ExpressionType();
	ExpressionType(const ExpressionType &) = delete;
	ExpressionType &operator=(const ExpressionType &) = delete;
	ExpressionType(ExpressionType &&) = default;
	ExpressionType &operator=(ExpressionType &&) = default;

public:
	// Deserialization
	static ExpressionType FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ExpressionType Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
