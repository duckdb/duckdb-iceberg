
#include "rest_catalog/objects/set_current_schema_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

SetCurrentSchemaUpdate::SetCurrentSchemaUpdate() {
}

SetCurrentSchemaUpdate SetCurrentSchemaUpdate::FromJSON(JSONValue obj) {
	SetCurrentSchemaUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SetCurrentSchemaUpdate SetCurrentSchemaUpdate::Copy() const {
	SetCurrentSchemaUpdate res;
	res.base_update = base_update.Copy();
	res.schema_id = schema_id;
	return res;
}

string SetCurrentSchemaUpdate::TryFromJSON(JSONValue obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto action_refinement_val = obj.GetMember("action");
	if (action_refinement_val.IsValid()) {
		string action_refinement;
		if (json_utils::IsString(action_refinement_val)) {
			action_refinement = json_utils::GetString(action_refinement_val);
		} else {
			return StringUtil::Format(
			    "SetCurrentSchemaUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "set-current-schema") {
			return "SetCurrentSchemaUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "SetCurrentSchemaUpdate required property 'action' is missing";
	}
	auto schema_id_val = obj.GetMember("schema-id");
	if (!schema_id_val.IsValid()) {
		return "SetCurrentSchemaUpdate required property 'schema-id' is missing";
	} else {
		if (json_utils::IsInteger(schema_id_val)) {
			schema_id = json_utils::GetSignedInteger(schema_id_val);
		} else {
			return StringUtil::Format(
			    "SetCurrentSchemaUpdate property 'schema_id' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(schema_id_val).c_str());
		}
	}
	return "";
}

void SetCurrentSchemaUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: schema-id
	auto schema_id_json = writer.CreateSignedInteger(schema_id);
	obj.Add("schema-id", schema_id_json);
}

JSONMutableValue SetCurrentSchemaUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
