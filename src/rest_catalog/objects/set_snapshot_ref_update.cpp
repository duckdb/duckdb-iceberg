
#include "rest_catalog/objects/set_snapshot_ref_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

SetSnapshotRefUpdate::SetSnapshotRefUpdate() {
}

SetSnapshotRefUpdate SetSnapshotRefUpdate::FromJSON(JSONValue obj) {
	SetSnapshotRefUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SetSnapshotRefUpdate SetSnapshotRefUpdate::Copy() const {
	SetSnapshotRefUpdate res;
	res.base_update = base_update.Copy();
	res.snapshot_reference = snapshot_reference.Copy();
	res.ref_name = ref_name;
	return res;
}

string SetSnapshotRefUpdate::TryFromJSON(JSONValue obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	error = snapshot_reference.TryFromJSON(obj);
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
			    "SetSnapshotRefUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "set-snapshot-ref") {
			return "SetSnapshotRefUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "SetSnapshotRefUpdate required property 'action' is missing";
	}
	auto ref_name_val = obj.GetMember("ref-name");
	if (!ref_name_val.IsValid()) {
		return "SetSnapshotRefUpdate required property 'ref-name' is missing";
	} else {
		if (json_utils::IsString(ref_name_val)) {
			ref_name = json_utils::GetString(ref_name_val);
		} else {
			return StringUtil::Format(
			    "SetSnapshotRefUpdate property 'ref_name' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(ref_name_val).c_str());
		}
	}
	return "";
}

void SetSnapshotRefUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize base class: SnapshotReference
	snapshot_reference.PopulateJSON(writer, obj);

	// Serialize: ref-name
	auto ref_name_json = writer.CreateString(ref_name);
	obj.Add("ref-name", ref_name_json);
}

JSONMutableValue SetSnapshotRefUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
