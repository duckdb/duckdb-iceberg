
#include "rest_catalog/objects/unary_expression.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

UnaryExpression::UnaryExpression() {
}

UnaryExpression UnaryExpression::FromJSON(yyjson_val *obj) {
	UnaryExpression res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string UnaryExpression::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "UnaryExpression required property 'type' is missing";
	} else {
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto term_val = yyjson_obj_get(obj, "term");
	if (!term_val) {
		return "UnaryExpression required property 'term' is missing";
	} else {
		error = term.TryFromJSON(term_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto value_val = yyjson_obj_get(obj, "value");
	if (!value_val) {
		return "UnaryExpression required property 'value' is missing";
	} else {
		if (yyjson_is_obj(value_val)) {
			value = value_val;
		} else {
			return "UnaryExpression property 'value' is not of type 'object'";
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
