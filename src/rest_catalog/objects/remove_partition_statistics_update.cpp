
#include "rest_catalog/objects/remove_partition_statistics_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

RemovePartitionStatisticsUpdate::RemovePartitionStatisticsUpdate() {
}

RemovePartitionStatisticsUpdate RemovePartitionStatisticsUpdate::FromJSON(yyjson_val *obj) {
	RemovePartitionStatisticsUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string RemovePartitionStatisticsUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "RemovePartitionStatisticsUpdate required property 'snapshot-id' is missing";
	} else {
		if (yyjson_is_sint(snapshot_id_val)) {
			snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else {
			return "RemovePartitionStatisticsUpdate property 'snapshot_id' is not of type 'integer'";
		}
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		has_action = true;
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return "RemovePartitionStatisticsUpdate property 'action' is not of type 'string'";
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
