
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class Predicate;
class ValueExpression;

class FunctionArgument {
public:
	FunctionArgument();
	FunctionArgument(const FunctionArgument &) = delete;
	FunctionArgument &operator=(const FunctionArgument &) = delete;
	FunctionArgument(FunctionArgument &&) = default;
	FunctionArgument &operator=(FunctionArgument &&) = default;

public:
	// Deserialization
	static FunctionArgument FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FunctionArgument Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	unique_ptr<ValueExpression> value_expression;
	unique_ptr<Predicate> predicate;
};

} // namespace rest_api_objects
} // namespace duckdb
