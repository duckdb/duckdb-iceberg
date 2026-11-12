
#include "rest_catalog/objects/id_reference.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

IdReference::IdReference() {
}

IdReference IdReference::FromJSON(JSONValue obj) {
	IdReference res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

IdReference IdReference::Copy() const {
	IdReference res;
	res.type = type;
	res.id = id;
	return res;
}

string IdReference::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "IdReference required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("IdReference property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "reference") {
			return "IdReference property 'type' does not match its required const value";
		}
	}
	auto id_val = obj.GetMember("id");
	if (!id_val.IsValid()) {
		return "IdReference required property 'id' is missing";
	} else {
		if (json_utils::IsInteger(id_val)) {
			id = json_utils::GetSignedInteger(id_val);
		} else {
			return StringUtil::Format("IdReference property 'id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(id_val).c_str());
		}
	}
	return "";
}

void IdReference::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: id
	auto id_json = writer.CreateSignedInteger(id);
	obj.Add("id", id_json);
}

JSONMutableValue IdReference::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
