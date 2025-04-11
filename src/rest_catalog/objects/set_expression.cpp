
#include "rest_catalog/objects/set_expression.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SetExpression::SetExpression() {
}

SetExpression SetExpression::FromJSON(yyjson_val *obj) {
	SetExpression res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string SetExpression::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "SetExpression required property 'type' is missing";
	} else {
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto term_val = yyjson_obj_get(obj, "term");
	if (!term_val) {
		return "SetExpression required property 'term' is missing";
	} else {
		error = term.TryFromJSON(term_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto values_val = yyjson_obj_get(obj, "values");
	if (!values_val) {
		return "SetExpression required property 'values' is missing";
	} else {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(values_val, idx, max, val) {
			auto tmp = val;
			values.emplace_back(std::move(tmp));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
