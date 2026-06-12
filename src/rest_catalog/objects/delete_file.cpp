
#include "rest_catalog/objects/delete_file.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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

DeleteFile DeleteFile::Copy() const {
	DeleteFile res;
	if (has_position_delete_file) {
		res.position_delete_file = position_delete_file.Copy();
	}
	res.has_position_delete_file = has_position_delete_file;
	if (has_equality_delete_file) {
		res.equality_delete_file = equality_delete_file.Copy();
	}
	res.has_equality_delete_file = has_equality_delete_file;
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
