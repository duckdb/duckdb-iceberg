
#include "rest_catalog/objects/function_representation.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FunctionRepresentation::FunctionRepresentation() {
}

FunctionRepresentation FunctionRepresentation::FromJSON(JSONValue obj) {
	FunctionRepresentation res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionRepresentation FunctionRepresentation::Copy() const {
	FunctionRepresentation res;
	if (function_sqlrepresentation.has_value()) {
		res.function_sqlrepresentation.emplace();
		(*res.function_sqlrepresentation) = (*function_sqlrepresentation).Copy();
	}
	return res;
}

string FunctionRepresentation::TryFromJSON(JSONValue obj) {
	string error;
	do {
		function_sqlrepresentation.emplace();
		error = function_sqlrepresentation->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			function_sqlrepresentation = nullopt;
		}
		return "FunctionRepresentation failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

void FunctionRepresentation::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	if (function_sqlrepresentation.has_value()) {
		function_sqlrepresentation->PopulateJSON(writer, obj);
	}
}

JSONMutableValue FunctionRepresentation::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
