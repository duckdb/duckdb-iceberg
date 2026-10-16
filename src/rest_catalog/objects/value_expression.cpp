
#include "rest_catalog/objects/value_expression.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ValueExpression::ValueExpression() {
}

ValueExpression ValueExpression::FromJSON(JSONValue obj) {
	ValueExpression res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ValueExpression ValueExpression::Copy() const {
	ValueExpression res;
	if (literal.has_value()) {
		res.literal.emplace();
		(*res.literal) = (*literal).Copy();
	}
	if (reference.has_value()) {
		res.reference.emplace();
		(*res.reference) = (*reference).Copy();
	}
	if (apply.has_value()) {
		res.apply.emplace();
		(*res.apply) = (*apply).Copy();
	}
	return res;
}

string ValueExpression::TryFromJSON(JSONValue obj) {
	string error;
	do {
		literal.emplace();
		error = literal->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			literal = nullopt;
		}
		reference.emplace();
		error = reference->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			reference = nullopt;
		}
		apply.emplace();
		error = apply->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			apply = nullopt;
		}
		return "ValueExpression failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

JSONMutableValue ValueExpression::ToJSON(JSONWriter &writer) const {
	if (literal.has_value()) {
		return literal->ToJSON(writer);
	} else if (reference.has_value()) {
		return reference->ToJSON(writer);
	} else if (apply.has_value()) {
		return apply->ToJSON(writer);
	}
	// No variant is active - return empty object
	return writer.CreateObject();
}

} // namespace rest_api_objects
} // namespace duckdb
