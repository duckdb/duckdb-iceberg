
#include "rest_catalog/objects/assert_last_assigned_partition_id.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

AssertLastAssignedPartitionId::AssertLastAssignedPartitionId() {
}

AssertLastAssignedPartitionId AssertLastAssignedPartitionId::FromJSON(JSONValue obj) {
	AssertLastAssignedPartitionId res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AssertLastAssignedPartitionId AssertLastAssignedPartitionId::Copy() const {
	AssertLastAssignedPartitionId res;
	res.type = type;
	res.last_assigned_partition_id = last_assigned_partition_id;
	return res;
}

string AssertLastAssignedPartitionId::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "AssertLastAssignedPartitionId required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format(
			    "AssertLastAssignedPartitionId property 'type' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "assert-last-assigned-partition-id") {
			return "AssertLastAssignedPartitionId property 'type' does not match its required const value";
		}
	}
	auto last_assigned_partition_id_val = obj.GetMember("last-assigned-partition-id");
	if (!last_assigned_partition_id_val.IsValid()) {
		return "AssertLastAssignedPartitionId required property 'last-assigned-partition-id' is missing";
	} else {
		if (json_utils::IsInteger(last_assigned_partition_id_val)) {
			last_assigned_partition_id = json_utils::GetSignedInteger(last_assigned_partition_id_val);
		} else {
			return StringUtil::Format("AssertLastAssignedPartitionId property 'last_assigned_partition_id' is not of "
			                          "type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(last_assigned_partition_id_val).c_str());
		}
	}
	return "";
}

void AssertLastAssignedPartitionId::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: last-assigned-partition-id
	auto last_assigned_partition_id_json = writer.CreateSignedInteger(last_assigned_partition_id);
	obj.Add("last-assigned-partition-id", last_assigned_partition_id_json);
}

JSONMutableValue AssertLastAssignedPartitionId::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
