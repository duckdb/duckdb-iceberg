
#include "rest_catalog/objects/view_requirement.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ViewRequirement::ViewRequirement() {
}

ViewRequirement ViewRequirement::FromJSON(yyjson_val *obj) {
	ViewRequirement res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string ViewRequirement::TryFromJSON(yyjson_val *obj) {
	string error;
	do {
		error = assert_view_uuid.TryFromJSON(obj);
		if (error.empty()) {
			has_assert_view_uuid = true;
			break;
		}
		return "ViewRequirement failed to parse, none of the oneOf candidates matched";
	} while (false);
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
