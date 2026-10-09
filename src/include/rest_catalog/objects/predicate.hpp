
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/and_or_predicate.hpp"
#include "rest_catalog/objects/comparison_predicate.hpp"
#include "rest_catalog/objects/false_expression.hpp"
#include "rest_catalog/objects/not_predicate.hpp"
#include "rest_catalog/objects/set_predicate.hpp"
#include "rest_catalog/objects/true_expression.hpp"
#include "rest_catalog/objects/unary_predicate.hpp"

namespace duckdb {
namespace rest_api_objects {

class Predicate {
public:
	Predicate();
	Predicate(const Predicate &) = delete;
	Predicate &operator=(const Predicate &) = delete;
	Predicate(Predicate &&) = default;
	Predicate &operator=(Predicate &&) = default;
	class PredicateOneOf1 {
	public:
		PredicateOneOf1();
		PredicateOneOf1(const PredicateOneOf1 &) = delete;
		PredicateOneOf1 &operator=(const PredicateOneOf1 &) = delete;
		PredicateOneOf1(PredicateOneOf1 &&) = default;
		PredicateOneOf1 &operator=(PredicateOneOf1 &&) = default;

	public:
		// Deserialization
		static PredicateOneOf1 FromJSON(JSONValue obj);
		string TryFromJSON(JSONValue obj);

		// Copy
		PredicateOneOf1 Copy() const;

		// Serialization
		JSONMutableValue ToJSON(JSONWriter &writer) const;

	public:
		bool value;
	};

public:
	// Deserialization
	static Predicate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	Predicate Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<PredicateOneOf1> predicate_one_of_1;
	optional<TrueExpression> true_expression;
	optional<FalseExpression> false_expression;
	optional<AndOrPredicate> and_or_predicate;
	optional<NotPredicate> not_predicate;
	optional<UnaryPredicate> unary_predicate;
	optional<ComparisonPredicate> comparison_predicate;
	optional<SetPredicate> set_predicate;
};

} // namespace rest_api_objects
} // namespace duckdb
