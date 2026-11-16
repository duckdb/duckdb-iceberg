
#include "rest_catalog/objects/register_view_request.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

RegisterViewRequest::RegisterViewRequest() {
}

RegisterViewRequest RegisterViewRequest::FromJSON(JSONValue obj) {
	RegisterViewRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

RegisterViewRequest RegisterViewRequest::Copy() const {
	RegisterViewRequest res;
	res.name = name;
	res.metadata_location = metadata_location;
	return res;
}

string RegisterViewRequest::TryFromJSON(JSONValue obj) {
	string error;
	auto name_val = obj.GetMember("name");
	if (!name_val.IsValid()) {
		return "RegisterViewRequest required property 'name' is missing";
	} else {
		if (json_utils::IsString(name_val)) {
			name = json_utils::GetString(name_val);
		} else {
			return StringUtil::Format("RegisterViewRequest property 'name' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(name_val).c_str());
		}
	}
	auto metadata_location_val = obj.GetMember("metadata-location");
	if (!metadata_location_val.IsValid()) {
		return "RegisterViewRequest required property 'metadata-location' is missing";
	} else {
		if (json_utils::IsString(metadata_location_val)) {
			metadata_location = json_utils::GetString(metadata_location_val);
		} else {
			return StringUtil::Format(
			    "RegisterViewRequest property 'metadata_location' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(metadata_location_val).c_str());
		}
	}
	return "";
}

void RegisterViewRequest::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: name
	auto name_json = writer.CreateString(name);
	obj.Add("name", name_json);

	// Serialize: metadata-location
	auto metadata_location_json = writer.CreateString(metadata_location);
	obj.Add("metadata-location", metadata_location_json);
}

JSONMutableValue RegisterViewRequest::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
