
#include "rest_catalog/objects/set_expression.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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

SetExpression SetExpression::Copy() const {
	SetExpression res;
	res.type = type.Copy();
	res.term = term.Copy();
	res.values.reserve(values.size());
	for (auto &item : values) {
		res.values.emplace_back(item.Copy());
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
		if (yyjson_is_arr(values_val)) {
			size_t values_idx, values_max;
			yyjson_val *values_item_val;
			yyjson_arr_foreach(values_val, values_idx, values_max, values_item_val) {
				PrimitiveTypeValue values_item;
				error = values_item.TryFromJSON(values_item_val);
				if (!error.empty()) {
					return error;
				}
				values.emplace_back(std::move(values_item));
			}
		} else {
			return StringUtil::Format("SetExpression property 'values' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(values_val));
		}
	}
	return "";
}

void SetExpression::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: type
	yyjson_mut_val *type_val = type.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "type", type_val);

	// Serialize: term
	yyjson_mut_val *term_val = term.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "term", term_val);

	// Serialize: values
	yyjson_mut_val *values_arr = yyjson_mut_arr(doc);
	for (const auto &item : values) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(values_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "values", values_arr);
}

yyjson_mut_val *SetExpression::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
