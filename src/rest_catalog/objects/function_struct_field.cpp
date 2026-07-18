
#include "rest_catalog/objects/function_struct_field.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FunctionStructField::FunctionStructField() {
}

FunctionStructField FunctionStructField::FromJSON(yyjson_val *obj) {
	FunctionStructField res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionStructField FunctionStructField::Copy() const {
	FunctionStructField res;
	res.name = name;
	res.type = type ? make_uniq<FunctionDataType>(type->Copy()) : nullptr;
	return res;
}

string FunctionStructField::TryFromJSON(yyjson_val *obj) {
	string error;
	auto name_val = yyjson_obj_get(obj, "name");
	if (!name_val) {
		return "FunctionStructField required property 'name' is missing";
	} else {
		if (yyjson_is_str(name_val)) {
			name = yyjson_get_str(name_val);
		} else {
			return StringUtil::Format("FunctionStructField property 'name' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(name_val));
		}
	}
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "FunctionStructField required property 'type' is missing";
	} else {
		type = make_uniq<FunctionDataType>();
		error = type->TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void FunctionStructField::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: name
	yyjson_mut_obj_add_strcpy(doc, obj, "name", name.c_str());

	// Serialize: type
	yyjson_mut_val *type_val = type->ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "type", type_val);
}

yyjson_mut_val *FunctionStructField::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
