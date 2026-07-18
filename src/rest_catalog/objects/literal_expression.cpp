
#include "rest_catalog/objects/literal_expression.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

LiteralExpression::LiteralExpression() {
}

LiteralExpression LiteralExpression::FromJSON(yyjson_val *obj) {
	LiteralExpression res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

LiteralExpression LiteralExpression::Copy() const {
	LiteralExpression res;
	res.type = type.Copy();
	res.term = term.Copy();
	res.value = value.Copy();
	return res;
}

string LiteralExpression::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "LiteralExpression required property 'type' is missing";
	} else {
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto term_val = yyjson_obj_get(obj, "term");
	if (!term_val) {
		return "LiteralExpression required property 'term' is missing";
	} else {
		error = term.TryFromJSON(term_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto value_val = yyjson_obj_get(obj, "value");
	if (!value_val) {
		return "LiteralExpression required property 'value' is missing";
	} else {
		error = value.TryFromJSON(value_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void LiteralExpression::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: type
	yyjson_mut_val *type_val = type.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "type", type_val);

	// Serialize: term
	yyjson_mut_val *term_val = term.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "term", term_val);

	// Serialize: value
	yyjson_mut_val *value_val = value.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "value", value_val);
}

yyjson_mut_val *LiteralExpression::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
