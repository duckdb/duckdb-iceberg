
#include "rest_catalog/objects/type.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

Type::Type() {
}

Type Type::FromJSON(yyjson_val *obj) {
	Type res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string Type::TryFromJSON(yyjson_val *obj) {
	string error;
	do {
		error = primitive_type.TryFromJSON(obj);
		if (error.empty()) {
			has_primitive_type = true;
			break;
		}
		error = struct_type.TryFromJSON(obj);
		if (error.empty()) {
			has_struct_type = true;
			break;
		}
		error = list_type.TryFromJSON(obj);
		if (error.empty()) {
			has_list_type = true;
			break;
		}
		error = map_type.TryFromJSON(obj);
		if (error.empty()) {
			has_map_type = true;
			break;
		}
		return "Type failed to parse, none of the oneOf candidates matched";
	} while (false);
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
