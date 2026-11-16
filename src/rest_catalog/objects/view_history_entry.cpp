
#include "rest_catalog/objects/view_history_entry.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ViewHistoryEntry::ViewHistoryEntry() {
}

ViewHistoryEntry ViewHistoryEntry::FromJSON(JSONValue obj) {
	ViewHistoryEntry res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ViewHistoryEntry ViewHistoryEntry::Copy() const {
	ViewHistoryEntry res;
	res.version_id = version_id;
	res.timestamp_ms = timestamp_ms;
	return res;
}

string ViewHistoryEntry::TryFromJSON(JSONValue obj) {
	string error;
	auto version_id_val = obj.GetMember("version-id");
	if (!version_id_val.IsValid()) {
		return "ViewHistoryEntry required property 'version-id' is missing";
	} else {
		if (json_utils::IsInteger(version_id_val)) {
			version_id = json_utils::GetSignedInteger(version_id_val);
		} else {
			return StringUtil::Format(
			    "ViewHistoryEntry property 'version_id' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(version_id_val).c_str());
		}
	}
	auto timestamp_ms_val = obj.GetMember("timestamp-ms");
	if (!timestamp_ms_val.IsValid()) {
		return "ViewHistoryEntry required property 'timestamp-ms' is missing";
	} else {
		if (json_utils::IsInteger(timestamp_ms_val)) {
			timestamp_ms = json_utils::GetSignedInteger(timestamp_ms_val);
		} else if (json_utils::IsUnsignedInteger(timestamp_ms_val)) {
			timestamp_ms = json_utils::GetUnsignedInteger(timestamp_ms_val);
		} else {
			return StringUtil::Format(
			    "ViewHistoryEntry property 'timestamp_ms' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(timestamp_ms_val).c_str());
		}
	}
	return "";
}

void ViewHistoryEntry::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: version-id
	auto version_id_json = writer.CreateSignedInteger(version_id);
	obj.Add("version-id", version_id_json);

	// Serialize: timestamp-ms
	auto timestamp_ms_json = writer.CreateSignedInteger(timestamp_ms);
	obj.Add("timestamp-ms", timestamp_ms_json);
}

JSONMutableValue ViewHistoryEntry::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
