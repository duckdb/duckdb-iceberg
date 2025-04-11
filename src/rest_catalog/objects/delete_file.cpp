
#include "rest_catalog/objects/delete_file.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

DeleteFile::DeleteFile() {
}

DeleteFile DeleteFile::FromJSON(yyjson_val *obj) {
	DeleteFile res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string DeleteFile::TryFromJSON(yyjson_val *obj) {
	string error;
	do {
		error = position_delete_file.TryFromJSON(obj);
		if (error.empty()) {
			has_position_delete_file = true;
			break;
		}
		error = equality_delete_file.TryFromJSON(obj);
		if (error.empty()) {
			has_equality_delete_file = true;
			break;
		}
		return "DeleteFile failed to parse, none of the oneOf candidates matched";
	} while (false);
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
