
#include "rest_catalog/objects/load_function_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

LoadFunctionResult::LoadFunctionResult() {
}

LoadFunctionResult LoadFunctionResult::FromJSON(yyjson_val *obj) {
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

string LoadFunctionResult::TryFromJSON(yyjson_val *obj) {
	string error;
	auto metadata_val = yyjson_obj_get(obj, "metadata");
	if (!metadata_val) {
		return "LoadFunctionResult required property 'metadata' is missing";
	} else {
		error = metadata.TryFromJSON(metadata_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto metadata_location_val = yyjson_obj_get(obj, "metadata-location");
	if (metadata_location_val) {
		string metadata_location_tmp;
		if (yyjson_is_str(metadata_location_val)) {
			metadata_location_tmp = yyjson_get_str(metadata_location_val);
		} else {
			return StringUtil::Format(
			    "LoadFunctionResult property 'metadata_location_tmp' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(metadata_location_val));
		}
		metadata_location = std::move(metadata_location_tmp);
	}
	return "";
}

void LoadFunctionResult::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: metadata
	yyjson_mut_val *metadata_val = metadata.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "metadata", metadata_val);

	// Serialize: metadata-location
	if (metadata_location.has_value()) {
		auto &metadata_location_value = *metadata_location;
		yyjson_mut_obj_add_strcpy(doc, obj, "metadata-location", metadata_location_value.c_str());
	}
}

yyjson_mut_val *LoadFunctionResult::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
