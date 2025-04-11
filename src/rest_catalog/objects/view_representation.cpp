
#include "rest_catalog/objects/view_representation.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ViewRepresentation::ViewRepresentation() {
}

ViewRepresentation ViewRepresentation::FromJSON(yyjson_val *obj) {
	ViewRepresentation res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string ViewRepresentation::TryFromJSON(yyjson_val *obj) {
	string error;
	do {
		error = sqlview_representation.TryFromJSON(obj);
		if (error.empty()) {
			has_sqlview_representation = true;
			break;
		}
		return "ViewRepresentation failed to parse, none of the oneOf candidates matched";
	} while (false);
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
