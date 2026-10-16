
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/expression_type.hpp"
#include "rest_catalog/objects/term.hpp"

namespace duckdb {
namespace rest_api_objects {

class ValueExpression;

class UnaryPredicate {
public:
	UnaryPredicate();
	UnaryPredicate(const UnaryPredicate &) = delete;
	UnaryPredicate &operator=(const UnaryPredicate &) = delete;
	UnaryPredicate(UnaryPredicate &&) = default;
	UnaryPredicate &operator=(UnaryPredicate &&) = default;

public:
	// Deserialization
	static UnaryPredicate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	UnaryPredicate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ExpressionType type;
	unique_ptr<ValueExpression> child;
	optional<Term> term;
};

} // namespace rest_api_objects
} // namespace duckdb
