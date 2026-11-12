
#include "rest_catalog/objects/snapshot_log.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

SnapshotLog::SnapshotLog() {
}
SnapshotLog::Object7::Object7() {
}

SnapshotLog::Object7 SnapshotLog::Object7::FromJSON(JSONValue obj) {
	Object7 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SnapshotLog::Object7 SnapshotLog::Object7::Copy() const {
	Object7 res;
	res.snapshot_id = snapshot_id;
	res.timestamp_ms = timestamp_ms;
	return res;
}

string SnapshotLog::Object7::TryFromJSON(JSONValue obj) {
	string error;
	auto snapshot_id_val = obj.GetMember("snapshot-id");
	if (!snapshot_id_val.IsValid()) {
		return "Object7 required property 'snapshot-id' is missing";
	} else {
		if (json_utils::IsInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetSignedInteger(snapshot_id_val);
		} else if (json_utils::IsUnsignedInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetUnsignedInteger(snapshot_id_val);
		} else {
			return StringUtil::Format("Object7 property 'snapshot_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(snapshot_id_val).c_str());
		}
	}
	auto timestamp_ms_val = obj.GetMember("timestamp-ms");
	if (!timestamp_ms_val.IsValid()) {
		return "Object7 required property 'timestamp-ms' is missing";
	} else {
		if (json_utils::IsInteger(timestamp_ms_val)) {
			timestamp_ms = json_utils::GetSignedInteger(timestamp_ms_val);
		} else if (json_utils::IsUnsignedInteger(timestamp_ms_val)) {
			timestamp_ms = json_utils::GetUnsignedInteger(timestamp_ms_val);
		} else {
			return StringUtil::Format("Object7 property 'timestamp_ms' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(timestamp_ms_val).c_str());
		}
	}
	return "";
}

void SnapshotLog::Object7::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: snapshot-id
	auto snapshot_id_json = writer.CreateSignedInteger(snapshot_id);
	obj.Add("snapshot-id", snapshot_id_json);

	// Serialize: timestamp-ms
	auto timestamp_ms_json = writer.CreateSignedInteger(timestamp_ms);
	obj.Add("timestamp-ms", timestamp_ms_json);
}

JSONMutableValue SnapshotLog::Object7::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

SnapshotLog SnapshotLog::FromJSON(JSONValue obj) {
	SnapshotLog res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SnapshotLog SnapshotLog::Copy() const {
	SnapshotLog res;
	res.value.reserve(value.size());
	for (auto &item : value) {
		res.value.emplace_back(item.Copy());
	}
	return res;
}

string SnapshotLog::TryFromJSON(JSONValue obj) {
	string error;
	if (obj.IsArray()) {
		obj.IterateArray([&](JSONValue value_item_val) {
			if (!error.empty()) {
				return;
			}
			Object7 value_item;
			error = value_item.TryFromJSON(value_item_val);
			if (!error.empty()) {
				return;
			}
			value.emplace_back(std::move(value_item));
		});
		if (!error.empty()) {
			return error;
		}
	} else {
		return StringUtil::Format("SnapshotLog property 'value' is not of type 'array', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	return "";
}

JSONMutableValue SnapshotLog::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateArray();
	for (const auto &result_item : value) {
		auto result_item_json = result_item.ToJSON(writer);
		result.Append(result_item_json);
	}
	return result;
}

} // namespace rest_api_objects
} // namespace duckdb
