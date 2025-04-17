
#include "rest_catalog/objects/commit_table_response.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

CommitTableResponse::CommitTableResponse() {
}

CommitTableResponse CommitTableResponse::FromJSON(yyjson_val *obj) {
	CommitTableResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string CommitTableResponse::TryFromJSON(yyjson_val *obj) {
	string error;
	auto metadata_location_val = yyjson_obj_get(obj, "metadata-location");
	if (!metadata_location_val) {
		return "CommitTableResponse required property 'metadata-location' is missing";
	} else {
		if (yyjson_is_str(metadata_location_val)) {
			metadata_location = yyjson_get_str(metadata_location_val);
		} else {
			return StringUtil::Format(
			    "CommitTableResponse property 'metadata_location' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(metadata_location_val));
		}
	}
	auto metadata_val = yyjson_obj_get(obj, "metadata");
	if (!metadata_val) {
		return "CommitTableResponse required property 'metadata' is missing";
	} else {
		error = metadata.TryFromJSON(metadata_val);
		if (!error.empty()) {
			return error;
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
