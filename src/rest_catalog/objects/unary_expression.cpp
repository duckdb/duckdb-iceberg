
#include "rest_catalog/objects/unary_expression.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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

UnaryExpression UnaryExpression::Copy() const {
	UnaryExpression res;
	res.type = type.Copy();
	res.term = term.Copy();
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
	return "";
}

yyjson_mut_val *UnaryExpression::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, obj, "type", type.ToJSON(doc));
	yyjson_mut_obj_add_val(doc, obj, "term", term.ToJSON(doc));
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
