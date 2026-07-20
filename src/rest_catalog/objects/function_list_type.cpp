
#include "rest_catalog/objects/function_list_type.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FunctionListType::FunctionListType() {
}

FunctionListType FunctionListType::FromJSON(yyjson_val *obj) {
	FunctionListType res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionListType FunctionListType::Copy() const {
	FunctionListType res;
	res.type = type;
	res.element = element ? make_uniq<FunctionDataType>(element->Copy()) : nullptr;
	return res;
}

string FunctionListType::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "FunctionListType required property 'type' is missing";
	} else {
		if (yyjson_is_str(type_val)) {
			type = yyjson_get_str(type_val);
		} else {
			return StringUtil::Format("FunctionListType property 'type' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(type_val));
		}
		if (!yyjson_is_null(type_val) && type != "list") {
			return "FunctionListType property 'type' does not match its required const value";
		}
	}
	auto element_val = yyjson_obj_get(obj, "element");
	if (!element_val) {
		return "FunctionListType required property 'element' is missing";
	} else {
		element = make_uniq<FunctionDataType>();
		error = element->TryFromJSON(element_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void FunctionListType::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: type
	yyjson_mut_obj_add_strcpy(doc, obj, "type", type.c_str());

	// Serialize: element
	yyjson_mut_val *element_val = element->ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "element", element_val);
}

yyjson_mut_val *FunctionListType::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
