
#include "rest_catalog/objects/named_reference.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

NamedReference::NamedReference() {
}

NamedReference NamedReference::FromJSON(JSONValue obj) {
	NamedReference res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

NamedReference NamedReference::Copy() const {
	NamedReference res;
	res.type = type;
	res.name = name;
	return res;
}

string NamedReference::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "NamedReference required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("NamedReference property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "reference") {
			return "NamedReference property 'type' does not match its required const value";
		}
	}
	auto name_val = obj.GetMember("name");
	if (!name_val.IsValid()) {
		return "NamedReference required property 'name' is missing";
	} else {
		if (json_utils::IsString(name_val)) {
			name = json_utils::GetString(name_val);
		} else {
			return StringUtil::Format("NamedReference property 'name' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(name_val).c_str());
		}
	}
	return "";
}

void NamedReference::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: name
	auto name_json = writer.CreateString(name);
	obj.Add("name", name_json);
}

JSONMutableValue NamedReference::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
