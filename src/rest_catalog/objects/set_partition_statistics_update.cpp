
#include "rest_catalog/objects/set_partition_statistics_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

SetPartitionStatisticsUpdate::SetPartitionStatisticsUpdate() {
}

SetPartitionStatisticsUpdate SetPartitionStatisticsUpdate::FromJSON(JSONValue obj) {
	SetPartitionStatisticsUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SetPartitionStatisticsUpdate SetPartitionStatisticsUpdate::Copy() const {
	SetPartitionStatisticsUpdate res;
	res.base_update = base_update.Copy();
	res.partition_statistics = partition_statistics.Copy();
	return res;
}

string SetPartitionStatisticsUpdate::TryFromJSON(JSONValue obj) {
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
			    "SetPartitionStatisticsUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "set-partition-statistics") {
			return "SetPartitionStatisticsUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "SetPartitionStatisticsUpdate required property 'action' is missing";
	}
	auto partition_statistics_val = obj.GetMember("partition-statistics");
	if (!partition_statistics_val.IsValid()) {
		return "SetPartitionStatisticsUpdate required property 'partition-statistics' is missing";
	} else {
		error = partition_statistics.TryFromJSON(partition_statistics_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void SetPartitionStatisticsUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: partition-statistics
	auto partition_statistics_json = partition_statistics.ToJSON(writer);
	obj.Add("partition-statistics", partition_statistics_json);
}

JSONMutableValue SetPartitionStatisticsUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
