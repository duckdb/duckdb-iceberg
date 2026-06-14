
#include "rest_catalog/objects/expression.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

Expression::Expression() {
}

Expression Expression::FromJSON(yyjson_val *obj) {
	Expression res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Expression Expression::Copy() const {
	Expression res;
	if (has_true_expression) {
		res.true_expression = true_expression.Copy();
	}
	res.has_true_expression = has_true_expression;
	if (has_false_expression) {
		res.false_expression = false_expression.Copy();
	}
	res.has_false_expression = has_false_expression;
	if (has_and_or_expression) {
		res.and_or_expression = and_or_expression.Copy();
	}
	res.has_and_or_expression = has_and_or_expression;
	if (has_not_expression) {
		res.not_expression = not_expression.Copy();
	}
	res.has_not_expression = has_not_expression;
	if (has_set_expression) {
		res.set_expression = set_expression.Copy();
	}
	res.has_set_expression = has_set_expression;
	if (has_literal_expression) {
		res.literal_expression = literal_expression.Copy();
	}
	res.has_literal_expression = has_literal_expression;
	if (has_unary_expression) {
		res.unary_expression = unary_expression.Copy();
	}
	res.has_unary_expression = has_unary_expression;
	return res;
}
string Expression::TryFromJSON(yyjson_val *obj) {
	string error;
	do {
		error = true_expression.TryFromJSON(obj);
		if (error.empty()) {
			has_true_expression = true;
			break;
		}
		error = false_expression.TryFromJSON(obj);
		if (error.empty()) {
			has_false_expression = true;
			break;
		}
		error = and_or_expression.TryFromJSON(obj);
		if (error.empty()) {
			has_and_or_expression = true;
			break;
		}
		error = not_expression.TryFromJSON(obj);
		if (error.empty()) {
			has_not_expression = true;
			break;
		}
		error = set_expression.TryFromJSON(obj);
		if (error.empty()) {
			has_set_expression = true;
			break;
		}
		error = literal_expression.TryFromJSON(obj);
		if (error.empty()) {
			has_literal_expression = true;
			break;
		}
		error = unary_expression.TryFromJSON(obj);
		if (error.empty()) {
			has_unary_expression = true;
			break;
		}
		return "Expression failed to parse, none of the oneOf candidates matched";
	} while (false);
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
