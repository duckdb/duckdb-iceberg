
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/expression.hpp"
#include "rest_catalog/objects/expression_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class NotExpression {
public:
	NotExpression() {
	}

public:
	static NotExpression FromJSON(yyjson_val *obj) {
		NotExpression res;
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
			return "NotExpression required property 'type' is missing";
		} else {
			error = type.TryFromJSON(type_val);
			if (!error.empty()) {
				return error;
			}
		}
		auto child_val = yyjson_obj_get(obj, "child");
		if (!child_val) {
			return "NotExpression required property 'child' is missing";
		} else {
			child = make_uniq<Expression>();
			error = child->TryFromJSON(child_val);
			if (!error.empty()) {
				return error;
			}
		}
		return string();
	}

public:
	ExpressionType type;
	unique_ptr<Expression> child;
};

} // namespace rest_api_objects
} // namespace duckdb
