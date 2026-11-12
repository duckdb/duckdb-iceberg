
#include "rest_catalog/objects/function_argument.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FunctionArgument::FunctionArgument() {
}

FunctionArgument FunctionArgument::FromJSON(JSONValue obj) {
	FunctionArgument res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionArgument FunctionArgument::Copy() const {
	FunctionArgument res;
	if (value_expression != nullptr) {
		res.value_expression = value_expression ? make_uniq<ValueExpression>(value_expression->Copy()) : nullptr;
	}
	if (predicate != nullptr) {
		res.predicate = predicate ? make_uniq<Predicate>(predicate->Copy()) : nullptr;
	}
	return res;
}

string FunctionArgument::TryFromJSON(JSONValue obj) {
	string error;
	value_expression = make_uniq<ValueExpression>();
	error = value_expression->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		value_expression = nullptr;
	}
	predicate = make_uniq<Predicate>();
	error = predicate->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		predicate = nullptr;
	}
	if (!(predicate != nullptr) && !(value_expression != nullptr)) {
		return "FunctionArgument failed to parse, none of the anyOf candidates matched";
	}
	return "";
}

JSONMutableValue FunctionArgument::ToJSON(JSONWriter &writer) const {
	if (value_expression != nullptr) {
		return value_expression->ToJSON(writer);
	} else if (predicate != nullptr) {
		return predicate->ToJSON(writer);
	}
	// No variant is active - return empty object
	return writer.CreateObject();
}

} // namespace rest_api_objects
} // namespace duckdb
