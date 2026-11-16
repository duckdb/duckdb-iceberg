
#include "rest_catalog/objects/set_statistics_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

SetStatisticsUpdate::SetStatisticsUpdate() {
}

SetStatisticsUpdate SetStatisticsUpdate::FromJSON(JSONValue obj) {
	SetStatisticsUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SetStatisticsUpdate SetStatisticsUpdate::Copy() const {
	SetStatisticsUpdate res;
	res.base_update = base_update.Copy();
	res.statistics = statistics.Copy();
	if (snapshot_id.has_value()) {
		res.snapshot_id.emplace();
		(*res.snapshot_id) = (*snapshot_id);
	}
	return res;
}

string SetStatisticsUpdate::TryFromJSON(JSONValue obj) {
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
			    "SetStatisticsUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "set-statistics") {
			return "SetStatisticsUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "SetStatisticsUpdate required property 'action' is missing";
	}
	auto statistics_val = obj.GetMember("statistics");
	if (!statistics_val.IsValid()) {
		return "SetStatisticsUpdate required property 'statistics' is missing";
	} else {
		error = statistics.TryFromJSON(statistics_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto snapshot_id_val = obj.GetMember("snapshot-id");
	if (snapshot_id_val.IsValid()) {
		int64_t snapshot_id_tmp;
		if (json_utils::IsInteger(snapshot_id_val)) {
			snapshot_id_tmp = json_utils::GetSignedInteger(snapshot_id_val);
		} else if (json_utils::IsUnsignedInteger(snapshot_id_val)) {
			snapshot_id_tmp = json_utils::GetUnsignedInteger(snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "SetStatisticsUpdate property 'snapshot_id_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(snapshot_id_val).c_str());
		}
		snapshot_id = std::move(snapshot_id_tmp);
	}
	return "";
}

void SetStatisticsUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: statistics
	auto statistics_json = statistics.ToJSON(writer);
	obj.Add("statistics", statistics_json);

	// Serialize: snapshot-id
	if (snapshot_id.has_value()) {
		auto &snapshot_id_value = *snapshot_id;
		auto snapshot_id_json = writer.CreateSignedInteger(snapshot_id_value);
		obj.Add("snapshot-id", snapshot_id_json);
	}
}

JSONMutableValue SetStatisticsUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
