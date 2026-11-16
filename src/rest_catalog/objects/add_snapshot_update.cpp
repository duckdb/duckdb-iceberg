
#include "rest_catalog/objects/add_snapshot_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

AddSnapshotUpdate::AddSnapshotUpdate() {
}

AddSnapshotUpdate AddSnapshotUpdate::FromJSON(JSONValue obj) {
	AddSnapshotUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AddSnapshotUpdate AddSnapshotUpdate::Copy() const {
	AddSnapshotUpdate res;
	res.base_update = base_update.Copy();
	res.snapshot = snapshot.Copy();
	return res;
}

string AddSnapshotUpdate::TryFromJSON(JSONValue obj) {
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
			    "AddSnapshotUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "add-snapshot") {
			return "AddSnapshotUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "AddSnapshotUpdate required property 'action' is missing";
	}
	auto snapshot_val = obj.GetMember("snapshot");
	if (!snapshot_val.IsValid()) {
		return "AddSnapshotUpdate required property 'snapshot' is missing";
	} else {
		error = snapshot.TryFromJSON(snapshot_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void AddSnapshotUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: snapshot
	auto snapshot_json = snapshot.ToJSON(writer);
	obj.Add("snapshot", snapshot_json);
}

JSONMutableValue AddSnapshotUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
