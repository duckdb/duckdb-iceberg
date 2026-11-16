
#include "rest_catalog/objects/remove_snapshots_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

RemoveSnapshotsUpdate::RemoveSnapshotsUpdate() {
}

RemoveSnapshotsUpdate RemoveSnapshotsUpdate::FromJSON(JSONValue obj) {
	RemoveSnapshotsUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

RemoveSnapshotsUpdate RemoveSnapshotsUpdate::Copy() const {
	RemoveSnapshotsUpdate res;
	res.base_update = base_update.Copy();
	res.snapshot_ids.reserve(snapshot_ids.size());
	for (auto &item : snapshot_ids) {
		res.snapshot_ids.emplace_back(item);
	}
	return res;
}

string RemoveSnapshotsUpdate::TryFromJSON(JSONValue obj) {
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
			    "RemoveSnapshotsUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "remove-snapshots") {
			return "RemoveSnapshotsUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "RemoveSnapshotsUpdate required property 'action' is missing";
	}
	auto snapshot_ids_val = obj.GetMember("snapshot-ids");
	if (!snapshot_ids_val.IsValid()) {
		return "RemoveSnapshotsUpdate required property 'snapshot-ids' is missing";
	} else {
		if (snapshot_ids_val.IsArray()) {
			snapshot_ids_val.IterateArray([&](JSONValue snapshot_ids_item_val) {
				if (!error.empty()) {
					return;
				}
				int64_t snapshot_ids_item;
				if (json_utils::IsInteger(snapshot_ids_item_val)) {
					snapshot_ids_item = json_utils::GetSignedInteger(snapshot_ids_item_val);
				} else if (json_utils::IsUnsignedInteger(snapshot_ids_item_val)) {
					snapshot_ids_item = json_utils::GetUnsignedInteger(snapshot_ids_item_val);
				} else {
					error = StringUtil::Format(
					    "RemoveSnapshotsUpdate property 'snapshot_ids_item' is not of type 'integer', found %s instead",
					    json_utils::GetTypeDescription(snapshot_ids_item_val).c_str());
					return;
				}
				snapshot_ids.emplace_back(std::move(snapshot_ids_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "RemoveSnapshotsUpdate property 'snapshot_ids' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(snapshot_ids_val).c_str());
		}
	}
	return "";
}

void RemoveSnapshotsUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: snapshot-ids
	auto snapshot_ids_json = writer.CreateArray();
	for (const auto &snapshot_ids_json_item : snapshot_ids) {
		auto snapshot_ids_json_item_json = writer.CreateSignedInteger(snapshot_ids_json_item);
		snapshot_ids_json.Append(snapshot_ids_json_item_json);
	}
	obj.Add("snapshot-ids", snapshot_ids_json);
}

JSONMutableValue RemoveSnapshotsUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
