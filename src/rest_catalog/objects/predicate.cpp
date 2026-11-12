
#include "rest_catalog/objects/predicate.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

Predicate::Predicate() {
}
Predicate::PredicateOneOf1::PredicateOneOf1() {
}

Predicate::PredicateOneOf1 Predicate::PredicateOneOf1::FromJSON(JSONValue obj) {
	PredicateOneOf1 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Predicate::PredicateOneOf1 Predicate::PredicateOneOf1::Copy() const {
	PredicateOneOf1 res;
	res.value = value;
	return res;
}

string Predicate::PredicateOneOf1::TryFromJSON(JSONValue obj) {
	string error;
	if (json_utils::IsBoolean(obj)) {
		value = json_utils::GetBoolean(obj);
	} else {
		return StringUtil::Format("PredicateOneOf1 property 'value' is not of type 'boolean', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	return "";
}

JSONMutableValue Predicate::PredicateOneOf1::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateBoolean(value);
	return result;
}

Predicate Predicate::FromJSON(JSONValue obj) {
	Predicate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Predicate Predicate::Copy() const {
	Predicate res;
	if (predicate_one_of_1.has_value()) {
		res.predicate_one_of_1.emplace();
		(*res.predicate_one_of_1) = (*predicate_one_of_1).Copy();
	}
	if (true_expression.has_value()) {
		res.true_expression.emplace();
		(*res.true_expression) = (*true_expression).Copy();
	}
	if (false_expression.has_value()) {
		res.false_expression.emplace();
		(*res.false_expression) = (*false_expression).Copy();
	}
	if (and_or_predicate.has_value()) {
		res.and_or_predicate.emplace();
		(*res.and_or_predicate) = (*and_or_predicate).Copy();
	}
	if (not_predicate.has_value()) {
		res.not_predicate.emplace();
		(*res.not_predicate) = (*not_predicate).Copy();
	}
	if (unary_predicate.has_value()) {
		res.unary_predicate.emplace();
		(*res.unary_predicate) = (*unary_predicate).Copy();
	}
	if (comparison_predicate.has_value()) {
		res.comparison_predicate.emplace();
		(*res.comparison_predicate) = (*comparison_predicate).Copy();
	}
	if (set_predicate.has_value()) {
		res.set_predicate.emplace();
		(*res.set_predicate) = (*set_predicate).Copy();
	}
	return res;
}

string Predicate::TryFromJSON(JSONValue obj) {
	string error;
	do {
		predicate_one_of_1.emplace();
		error = predicate_one_of_1->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			predicate_one_of_1 = nullopt;
		}
		true_expression.emplace();
		error = true_expression->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			true_expression = nullopt;
		}
		false_expression.emplace();
		error = false_expression->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			false_expression = nullopt;
		}
		and_or_predicate.emplace();
		error = and_or_predicate->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			and_or_predicate = nullopt;
		}
		not_predicate.emplace();
		error = not_predicate->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			not_predicate = nullopt;
		}
		unary_predicate.emplace();
		error = unary_predicate->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			unary_predicate = nullopt;
		}
		comparison_predicate.emplace();
		error = comparison_predicate->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			comparison_predicate = nullopt;
		}
		set_predicate.emplace();
		error = set_predicate->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			set_predicate = nullopt;
		}
		return "Predicate failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

JSONMutableValue Predicate::ToJSON(JSONWriter &writer) const {
	if (predicate_one_of_1.has_value()) {
		return predicate_one_of_1->ToJSON(writer);
	} else if (true_expression.has_value()) {
		return true_expression->ToJSON(writer);
	} else if (false_expression.has_value()) {
		return false_expression->ToJSON(writer);
	} else if (and_or_predicate.has_value()) {
		return and_or_predicate->ToJSON(writer);
	} else if (not_predicate.has_value()) {
		return not_predicate->ToJSON(writer);
	} else if (unary_predicate.has_value()) {
		return unary_predicate->ToJSON(writer);
	} else if (comparison_predicate.has_value()) {
		return comparison_predicate->ToJSON(writer);
	} else if (set_predicate.has_value()) {
		return set_predicate->ToJSON(writer);
	}
	// No variant is active - return empty object
	return writer.CreateObject();
}

} // namespace rest_api_objects
} // namespace duckdb
