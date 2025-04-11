
#include "rest_catalog/objects/primitive_type.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

PrimitiveType::PrimitiveType() {
}

PrimitiveType PrimitiveType::FromJSON(yyjson_val *obj) {
	PrimitiveType res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string PrimitiveType::TryFromJSON(yyjson_val *obj) {
	string error;
	value = yyjson_get_str(obj);
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
