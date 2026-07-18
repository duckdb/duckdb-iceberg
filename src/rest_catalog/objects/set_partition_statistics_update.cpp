
#include "rest_catalog/objects/set_partition_statistics_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SetPartitionStatisticsUpdate::SetPartitionStatisticsUpdate() {
}

SetPartitionStatisticsUpdate SetPartitionStatisticsUpdate::FromJSON(yyjson_val *obj) {
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

string SetPartitionStatisticsUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto action_refinement_val = yyjson_obj_get(obj, "action");
	if (action_refinement_val) {
		string action_refinement;
		if (yyjson_is_str(action_refinement_val)) {
			action_refinement = yyjson_get_str(action_refinement_val);
		} else {
			return StringUtil::Format(
			    "SetPartitionStatisticsUpdate property 'action_refinement' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_refinement_val));
		}
		if (!yyjson_is_null(action_refinement_val) && action_refinement != "set-partition-statistics") {
			return "SetPartitionStatisticsUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "SetPartitionStatisticsUpdate required property 'action' is missing";
	}
	auto partition_statistics_val = yyjson_obj_get(obj, "partition-statistics");
	if (!partition_statistics_val) {
		return "SetPartitionStatisticsUpdate required property 'partition-statistics' is missing";
	} else {
		error = partition_statistics.TryFromJSON(partition_statistics_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void SetPartitionStatisticsUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: partition-statistics
	yyjson_mut_val *partition_statistics_val = partition_statistics.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "partition-statistics", partition_statistics_val);
}

yyjson_mut_val *SetPartitionStatisticsUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
