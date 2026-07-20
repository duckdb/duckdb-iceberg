
#include "rest_catalog/objects/function_representation.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FunctionRepresentation::FunctionRepresentation() {
}

FunctionRepresentation FunctionRepresentation::FromJSON(yyjson_val *obj) {
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

string FunctionRepresentation::TryFromJSON(yyjson_val *obj) {
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

void FunctionRepresentation::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	if (function_sqlrepresentation.has_value()) {
		function_sqlrepresentation->PopulateJSON(doc, obj);
	}
}

yyjson_mut_val *FunctionRepresentation::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
