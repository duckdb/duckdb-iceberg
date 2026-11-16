
#include "rest_catalog/objects/remove_partition_statistics_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

RemovePartitionStatisticsUpdate::RemovePartitionStatisticsUpdate() {
}

RemovePartitionStatisticsUpdate RemovePartitionStatisticsUpdate::FromJSON(JSONValue obj) {
	RemovePartitionStatisticsUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

RemovePartitionStatisticsUpdate RemovePartitionStatisticsUpdate::Copy() const {
	RemovePartitionStatisticsUpdate res;
	res.base_update = base_update.Copy();
	res.snapshot_id = snapshot_id;
	return res;
}

string RemovePartitionStatisticsUpdate::TryFromJSON(JSONValue obj) {
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
			return StringUtil::Format("RemovePartitionStatisticsUpdate property 'action_refinement' is not of type "
			                          "'string', found %s instead",
			                          json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "remove-partition-statistics") {
			return "RemovePartitionStatisticsUpdate property 'action_refinement' does not match its required const "
			       "value";
		}
	} else {
		return "RemovePartitionStatisticsUpdate required property 'action' is missing";
	}
	auto snapshot_id_val = obj.GetMember("snapshot-id");
	if (!snapshot_id_val.IsValid()) {
		return "RemovePartitionStatisticsUpdate required property 'snapshot-id' is missing";
	} else {
		if (json_utils::IsInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetSignedInteger(snapshot_id_val);
		} else if (json_utils::IsUnsignedInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetUnsignedInteger(snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "RemovePartitionStatisticsUpdate property 'snapshot_id' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(snapshot_id_val).c_str());
		}
	}
	return "";
}

void RemovePartitionStatisticsUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: snapshot-id
	auto snapshot_id_json = writer.CreateSignedInteger(snapshot_id);
	obj.Add("snapshot-id", snapshot_id_json);
}

JSONMutableValue RemovePartitionStatisticsUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
