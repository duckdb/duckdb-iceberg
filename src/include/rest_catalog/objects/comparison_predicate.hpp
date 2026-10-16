
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/expression_type.hpp"
#include "rest_catalog/objects/literal.hpp"
#include "rest_catalog/objects/term.hpp"

namespace duckdb {
namespace rest_api_objects {

class ValueExpression;

class ComparisonPredicate {
public:
	ComparisonPredicate();
	ComparisonPredicate(const ComparisonPredicate &) = delete;
	ComparisonPredicate &operator=(const ComparisonPredicate &) = delete;
	ComparisonPredicate(ComparisonPredicate &&) = default;
	ComparisonPredicate &operator=(ComparisonPredicate &&) = default;

public:
	// Deserialization
	static ComparisonPredicate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ComparisonPredicate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ExpressionType type;
	unique_ptr<ValueExpression> left;
	unique_ptr<ValueExpression> right;
	optional<Term> term;
	optional<Literal> value;
};

} // namespace rest_api_objects
} // namespace duckdb
