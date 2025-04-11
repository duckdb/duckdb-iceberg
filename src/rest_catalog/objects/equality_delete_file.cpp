
#include "rest_catalog/objects/equality_delete_file.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

EqualityDeleteFile::EqualityDeleteFile() {
}

EqualityDeleteFile EqualityDeleteFile::FromJSON(yyjson_val *obj) {
	EqualityDeleteFile res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string EqualityDeleteFile::TryFromJSON(yyjson_val *obj) {
	string error;
	error = content_file.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto content_val = yyjson_obj_get(obj, "content");
	if (!content_val) {
		return "EqualityDeleteFile required property 'content' is missing";
	} else {
		content = yyjson_get_str(content_val);
	}
	auto equality_ids_val = yyjson_obj_get(obj, "equality-ids");
	if (equality_ids_val) {
		has_equality_ids = true;
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(equality_ids_val, idx, max, val) {
			auto tmp = yyjson_get_sint(val);
			equality_ids.emplace_back(std::move(tmp));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
