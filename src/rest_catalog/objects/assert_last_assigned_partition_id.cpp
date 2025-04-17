
#include "rest_catalog/objects/assert_last_assigned_partition_id.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AssertLastAssignedPartitionId::AssertLastAssignedPartitionId() {
}

AssertLastAssignedPartitionId AssertLastAssignedPartitionId::FromJSON(yyjson_val *obj) {
	AssertLastAssignedPartitionId res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string AssertLastAssignedPartitionId::TryFromJSON(yyjson_val *obj) {
	string error;
	error = table_requirement.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto last_assigned_partition_id_val = yyjson_obj_get(obj, "last-assigned-partition-id");
	if (!last_assigned_partition_id_val) {
		return "AssertLastAssignedPartitionId required property 'last-assigned-partition-id' is missing";
	} else {
		if (yyjson_is_int(last_assigned_partition_id_val)) {
			last_assigned_partition_id = yyjson_get_int(last_assigned_partition_id_val);
		} else {
			return StringUtil::Format("AssertLastAssignedPartitionId property 'last_assigned_partition_id' is not of "
			                          "type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(last_assigned_partition_id_val));
		}
	}
	auto type_val = yyjson_obj_get(obj, "type");
	if (type_val) {
		has_type = true;
		if (yyjson_is_str(type_val)) {
			type = yyjson_get_str(type_val);
		} else {
			return StringUtil::Format(
			    "AssertLastAssignedPartitionId property 'type' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(type_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
