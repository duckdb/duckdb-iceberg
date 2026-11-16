
#include "rest_catalog/objects/set_default_spec_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

SetDefaultSpecUpdate::SetDefaultSpecUpdate() {
}

SetDefaultSpecUpdate SetDefaultSpecUpdate::FromJSON(JSONValue obj) {
	SetDefaultSpecUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SetDefaultSpecUpdate SetDefaultSpecUpdate::Copy() const {
	SetDefaultSpecUpdate res;
	res.base_update = base_update.Copy();
	res.spec_id = spec_id;
	return res;
}

string SetDefaultSpecUpdate::TryFromJSON(JSONValue obj) {
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
			    "SetDefaultSpecUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "set-default-spec") {
			return "SetDefaultSpecUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "SetDefaultSpecUpdate required property 'action' is missing";
	}
	auto spec_id_val = obj.GetMember("spec-id");
	if (!spec_id_val.IsValid()) {
		return "SetDefaultSpecUpdate required property 'spec-id' is missing";
	} else {
		if (json_utils::IsInteger(spec_id_val)) {
			spec_id = json_utils::GetSignedInteger(spec_id_val);
		} else {
			return StringUtil::Format(
			    "SetDefaultSpecUpdate property 'spec_id' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(spec_id_val).c_str());
		}
	}
	return "";
}

void SetDefaultSpecUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: spec-id
	auto spec_id_json = writer.CreateSignedInteger(spec_id);
	obj.Add("spec-id", spec_id_json);
}

JSONMutableValue SetDefaultSpecUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
