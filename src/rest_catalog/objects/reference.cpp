
#include "rest_catalog/objects/reference.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

Reference::Reference() {
}

Reference Reference::FromJSON(yyjson_val *obj) {
	Reference res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string Reference::TryFromJSON(yyjson_val *obj) {
	string error;
	value = yyjson_get_str(obj);
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
