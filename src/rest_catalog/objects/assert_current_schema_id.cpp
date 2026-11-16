
#include "rest_catalog/objects/assert_current_schema_id.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

AssertCurrentSchemaId::AssertCurrentSchemaId() {
}

AssertCurrentSchemaId AssertCurrentSchemaId::FromJSON(JSONValue obj) {
	AssertCurrentSchemaId res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AssertCurrentSchemaId AssertCurrentSchemaId::Copy() const {
	AssertCurrentSchemaId res;
	res.type = type;
	res.current_schema_id = current_schema_id;
	return res;
}

string AssertCurrentSchemaId::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "AssertCurrentSchemaId required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("AssertCurrentSchemaId property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "assert-current-schema-id") {
			return "AssertCurrentSchemaId property 'type' does not match its required const value";
		}
	}
	auto current_schema_id_val = obj.GetMember("current-schema-id");
	if (!current_schema_id_val.IsValid()) {
		return "AssertCurrentSchemaId required property 'current-schema-id' is missing";
	} else {
		if (json_utils::IsInteger(current_schema_id_val)) {
			current_schema_id = json_utils::GetSignedInteger(current_schema_id_val);
		} else {
			return StringUtil::Format(
			    "AssertCurrentSchemaId property 'current_schema_id' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(current_schema_id_val).c_str());
		}
	}
	return "";
}

void AssertCurrentSchemaId::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: current-schema-id
	auto current_schema_id_json = writer.CreateSignedInteger(current_schema_id);
	obj.Add("current-schema-id", current_schema_id_json);
}

JSONMutableValue AssertCurrentSchemaId::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
