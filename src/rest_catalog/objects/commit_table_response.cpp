
#include "rest_catalog/objects/commit_table_response.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

CommitTableResponse::CommitTableResponse() {
}

CommitTableResponse CommitTableResponse::FromJSON(JSONValue obj) {
	CommitTableResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CommitTableResponse CommitTableResponse::Copy() const {
	CommitTableResponse res;
	res.metadata_location = metadata_location;
	res.metadata = metadata.Copy();
	return res;
}

string CommitTableResponse::TryFromJSON(JSONValue obj) {
	string error;
	auto metadata_location_val = obj.GetMember("metadata-location");
	if (!metadata_location_val.IsValid()) {
		return "CommitTableResponse required property 'metadata-location' is missing";
	} else {
		if (json_utils::IsString(metadata_location_val)) {
			metadata_location = json_utils::GetString(metadata_location_val);
		} else {
			return StringUtil::Format(
			    "CommitTableResponse property 'metadata_location' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(metadata_location_val).c_str());
		}
	}
	auto metadata_val = obj.GetMember("metadata");
	if (!metadata_val.IsValid()) {
		return "CommitTableResponse required property 'metadata' is missing";
	} else {
		error = metadata.TryFromJSON(metadata_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void CommitTableResponse::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: metadata-location
	auto metadata_location_json = writer.CreateString(metadata_location);
	obj.Add("metadata-location", metadata_location_json);

	// Serialize: metadata
	auto metadata_json = metadata.ToJSON(writer);
	obj.Add("metadata", metadata_json);
}

JSONMutableValue CommitTableResponse::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
