
#include "rest_catalog/objects/position_delete_file.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

PositionDeleteFile::PositionDeleteFile() {
}

PositionDeleteFile PositionDeleteFile::FromJSON(yyjson_val *obj) {
	PositionDeleteFile res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string PositionDeleteFile::TryFromJSON(yyjson_val *obj) {
	string error;
	error = content_file.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto content_val = yyjson_obj_get(obj, "content");
	if (!content_val) {
		return "PositionDeleteFile required property 'content' is missing";
	} else {
		content = yyjson_get_str(content_val);
	}
	auto content_offset_val = yyjson_obj_get(obj, "content-offset");
	if (content_offset_val) {
		has_content_offset = true;
		content_offset = yyjson_get_sint(content_offset_val);
	}
	auto content_size_in_bytes_val = yyjson_obj_get(obj, "content-size-in-bytes");
	if (content_size_in_bytes_val) {
		has_content_size_in_bytes = true;
		content_size_in_bytes = yyjson_get_sint(content_size_in_bytes_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
