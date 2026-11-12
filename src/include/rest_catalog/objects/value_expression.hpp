
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/apply.hpp"
#include "rest_catalog/objects/literal.hpp"
#include "rest_catalog/objects/reference.hpp"

namespace duckdb {
namespace rest_api_objects {

class ValueExpression {
public:
	ValueExpression();
	ValueExpression(const ValueExpression &) = delete;
	ValueExpression &operator=(const ValueExpression &) = delete;
	ValueExpression(ValueExpression &&) = default;
	ValueExpression &operator=(ValueExpression &&) = default;

public:
	// Deserialization
	static ValueExpression FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ValueExpression Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<Literal> literal;
	optional<Reference> reference;
	optional<Apply> apply;
};

} // namespace rest_api_objects
} // namespace duckdb
