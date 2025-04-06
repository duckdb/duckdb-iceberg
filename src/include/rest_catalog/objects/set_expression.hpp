
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/expression_type.hpp"
#include "rest_catalog/objects/term.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SetExpression {
public:
	SetExpression() {
	}

public:
	static SetExpression FromJSON(yyjson_val *obj) {
		SetExpression res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
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
				values.push_back(tmp);
			}
		}

		return string();
	}

public:
public:
	Term term;
	ExpressionType type;
	vector<yyjson_val *> values;
};

} // namespace rest_api_objects
} // namespace duckdb
