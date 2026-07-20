
#include "rest_catalog/objects/function_parameter.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FunctionParameter::FunctionParameter() {
}

FunctionParameter FunctionParameter::FromJSON(yyjson_val *obj) {
	FunctionParameter res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionParameter FunctionParameter::Copy() const {
	FunctionParameter res;
	res.type = type ? make_uniq<FunctionDataType>(type->Copy()) : nullptr;
	res.name = name;
	if (_doc.has_value()) {
		res._doc.emplace();
		(*res._doc) = (*_doc);
	}
	return res;
}

string FunctionParameter::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "FunctionParameter required property 'type' is missing";
	} else {
		type = make_uniq<FunctionDataType>();
		error = type->TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto name_val = yyjson_obj_get(obj, "name");
	if (!name_val) {
		return "FunctionParameter required property 'name' is missing";
	} else {
		if (yyjson_is_str(name_val)) {
			name = yyjson_get_str(name_val);
		} else {
			return StringUtil::Format("FunctionParameter property 'name' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(name_val));
		}
	}
	auto _doc_val = yyjson_obj_get(obj, "doc");
	if (_doc_val) {
		string _doc_tmp;
		if (yyjson_is_str(_doc_val)) {
			_doc_tmp = yyjson_get_str(_doc_val);
		} else {
			return StringUtil::Format(
			    "FunctionParameter property '_doc_tmp' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(_doc_val));
		}
		_doc = std::move(_doc_tmp);
	}
	return "";
}

void FunctionParameter::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: type
	yyjson_mut_val *type_val = type->ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "type", type_val);

	// Serialize: name
	yyjson_mut_obj_add_strcpy(doc, obj, "name", name.c_str());

	// Serialize: doc
	if (_doc.has_value()) {
		auto &_doc_value = *_doc;
		yyjson_mut_obj_add_strcpy(doc, obj, "doc", _doc_value.c_str());
	}
}

yyjson_mut_val *FunctionParameter::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
