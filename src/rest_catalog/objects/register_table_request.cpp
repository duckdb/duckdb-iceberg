
#include "rest_catalog/objects/register_table_request.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

RegisterTableRequest::RegisterTableRequest() {
}

RegisterTableRequest RegisterTableRequest::FromJSON(JSONValue obj) {
	RegisterTableRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

RegisterTableRequest RegisterTableRequest::Copy() const {
	RegisterTableRequest res;
	res.name = name;
	res.metadata_location = metadata_location;
	if (overwrite.has_value()) {
		res.overwrite.emplace();
		(*res.overwrite) = (*overwrite);
	}
	return res;
}

string RegisterTableRequest::TryFromJSON(JSONValue obj) {
	string error;
	auto name_val = obj.GetMember("name");
	if (!name_val.IsValid()) {
		return "RegisterTableRequest required property 'name' is missing";
	} else {
		if (json_utils::IsString(name_val)) {
			name = json_utils::GetString(name_val);
		} else {
			return StringUtil::Format("RegisterTableRequest property 'name' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(name_val).c_str());
		}
	}
	auto metadata_location_val = obj.GetMember("metadata-location");
	if (!metadata_location_val.IsValid()) {
		return "RegisterTableRequest required property 'metadata-location' is missing";
	} else {
		if (json_utils::IsString(metadata_location_val)) {
			metadata_location = json_utils::GetString(metadata_location_val);
		} else {
			return StringUtil::Format(
			    "RegisterTableRequest property 'metadata_location' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(metadata_location_val).c_str());
		}
	}
	auto overwrite_val = obj.GetMember("overwrite");
	if (overwrite_val.IsValid()) {
		bool overwrite_tmp;
		if (json_utils::IsBoolean(overwrite_val)) {
			overwrite_tmp = json_utils::GetBoolean(overwrite_val);
		} else {
			return StringUtil::Format(
			    "RegisterTableRequest property 'overwrite_tmp' is not of type 'boolean', found %s instead",
			    json_utils::GetTypeDescription(overwrite_val).c_str());
		}
		overwrite = std::move(overwrite_tmp);
	}
	return "";
}

void RegisterTableRequest::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: name
	auto name_json = writer.CreateString(name);
	obj.Add("name", name_json);

	// Serialize: metadata-location
	auto metadata_location_json = writer.CreateString(metadata_location);
	obj.Add("metadata-location", metadata_location_json);

	// Serialize: overwrite
	if (overwrite.has_value()) {
		auto &overwrite_value = *overwrite;
		auto overwrite_json = writer.CreateBoolean(overwrite_value);
		obj.Add("overwrite", overwrite_json);
	}
}

JSONMutableValue RegisterTableRequest::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
