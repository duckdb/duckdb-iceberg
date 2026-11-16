
#include "rest_catalog/objects/load_function_result.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

LoadFunctionResult::LoadFunctionResult() {
}

LoadFunctionResult LoadFunctionResult::FromJSON(JSONValue obj) {
	LoadFunctionResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

LoadFunctionResult LoadFunctionResult::Copy() const {
	LoadFunctionResult res;
	res.metadata = metadata.Copy();
	if (metadata_location.has_value()) {
		res.metadata_location.emplace();
		(*res.metadata_location) = (*metadata_location);
	}
	return res;
}

string LoadFunctionResult::TryFromJSON(JSONValue obj) {
	string error;
	auto metadata_val = obj.GetMember("metadata");
	if (!metadata_val.IsValid()) {
		return "LoadFunctionResult required property 'metadata' is missing";
	} else {
		error = metadata.TryFromJSON(metadata_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto metadata_location_val = obj.GetMember("metadata-location");
	if (metadata_location_val.IsValid()) {
		string metadata_location_tmp;
		if (json_utils::IsString(metadata_location_val)) {
			metadata_location_tmp = json_utils::GetString(metadata_location_val);
		} else {
			return StringUtil::Format(
			    "LoadFunctionResult property 'metadata_location_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(metadata_location_val).c_str());
		}
		metadata_location = std::move(metadata_location_tmp);
	}
	return "";
}

void LoadFunctionResult::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: metadata
	auto metadata_json = metadata.ToJSON(writer);
	obj.Add("metadata", metadata_json);

	// Serialize: metadata-location
	if (metadata_location.has_value()) {
		auto &metadata_location_value = *metadata_location;
		auto metadata_location_json = writer.CreateString(metadata_location_value);
		obj.Add("metadata-location", metadata_location_json);
	}
}

JSONMutableValue LoadFunctionResult::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
