
#include "rest_catalog/objects/list_type.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ListType::ListType() {
}

ListType ListType::FromJSON(JSONValue obj) {
	ListType res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ListType ListType::Copy() const {
	ListType res;
	res.type = type;
	res.element_id = element_id;
	res.element = element ? make_uniq<Type>(element->Copy()) : nullptr;
	res.element_required = element_required;
	return res;
}

string ListType::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "ListType required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("ListType property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "list") {
			return "ListType property 'type' does not match its required const value";
		}
	}
	auto element_id_val = obj.GetMember("element-id");
	if (!element_id_val.IsValid()) {
		return "ListType required property 'element-id' is missing";
	} else {
		if (json_utils::IsInteger(element_id_val)) {
			element_id = json_utils::GetSignedInteger(element_id_val);
		} else {
			return StringUtil::Format("ListType property 'element_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(element_id_val).c_str());
		}
	}
	auto element_val = obj.GetMember("element");
	if (!element_val.IsValid()) {
		return "ListType required property 'element' is missing";
	} else {
		element = make_uniq<Type>();
		error = element->TryFromJSON(element_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto element_required_val = obj.GetMember("element-required");
	if (!element_required_val.IsValid()) {
		return "ListType required property 'element-required' is missing";
	} else {
		if (json_utils::IsBoolean(element_required_val)) {
			element_required = json_utils::GetBoolean(element_required_val);
		} else {
			return StringUtil::Format("ListType property 'element_required' is not of type 'boolean', found %s instead",
			                          json_utils::GetTypeDescription(element_required_val).c_str());
		}
	}
	return "";
}

void ListType::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: element-id
	auto element_id_json = writer.CreateSignedInteger(element_id);
	obj.Add("element-id", element_id_json);

	// Serialize: element
	auto element_json = element->ToJSON(writer);
	obj.Add("element", element_json);

	// Serialize: element-required
	auto element_required_json = writer.CreateBoolean(element_required);
	obj.Add("element-required", element_required_json);
}

JSONMutableValue ListType::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
