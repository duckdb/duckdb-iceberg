
#include "rest_catalog/objects/and_or_expression.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AndOrExpression::AndOrExpression() {
}

AndOrExpression AndOrExpression::FromJSON(yyjson_val *obj) {
	AndOrExpression res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string AndOrExpression::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "AndOrExpression required property 'type' is missing";
	} else {
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto left_val = yyjson_obj_get(obj, "left");
	if (!left_val) {
		return "AndOrExpression required property 'left' is missing";
	} else {
		left = make_uniq<Expression>();
		error = left->TryFromJSON(left_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto right_val = yyjson_obj_get(obj, "right");
	if (!right_val) {
		return "AndOrExpression required property 'right' is missing";
	} else {
		right = make_uniq<Expression>();
		error = right->TryFromJSON(right_val);
		if (!error.empty()) {
			return error;
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
