
#include "rest_catalog/objects/function_parameter.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FunctionParameter::FunctionParameter() {
}

FunctionParameter FunctionParameter::FromJSON(JSONValue obj) {
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

string FunctionParameter::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "FunctionParameter required property 'type' is missing";
	} else {
		type = make_uniq<FunctionDataType>();
		error = type->TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto name_val = obj.GetMember("name");
	if (!name_val.IsValid()) {
		return "FunctionParameter required property 'name' is missing";
	} else {
		if (json_utils::IsString(name_val)) {
			name = json_utils::GetString(name_val);
		} else {
			return StringUtil::Format("FunctionParameter property 'name' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(name_val).c_str());
		}
	}
	auto _doc_val = obj.GetMember("doc");
	if (_doc_val.IsValid()) {
		string _doc_tmp;
		if (json_utils::IsString(_doc_val)) {
			_doc_tmp = json_utils::GetString(_doc_val);
		} else {
			return StringUtil::Format("FunctionParameter property '_doc_tmp' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(_doc_val).c_str());
		}
		_doc = std::move(_doc_tmp);
	}
	return "";
}

void FunctionParameter::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = type->ToJSON(writer);
	obj.Add("type", type_json);

	// Serialize: name
	auto name_json = writer.CreateString(name);
	obj.Add("name", name_json);

	// Serialize: doc
	if (_doc.has_value()) {
		auto &_doc_value = *_doc;
		auto _doc_json = writer.CreateString(_doc_value);
		obj.Add("doc", _doc_json);
	}
}

JSONMutableValue FunctionParameter::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
