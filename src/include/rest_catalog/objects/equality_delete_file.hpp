#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/content_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class EqualityDeleteFile {
public:
	static EqualityDeleteFile FromJSON(yyjson_val *obj) {
		EqualityDeleteFile result;

		// Parse ContentFile fields
		result.content_file = ContentFile::FromJSON(obj);

		auto content_val = yyjson_obj_get(obj, "content");
		if (content_val) {
			result.content = yyjson_get_str(content_val);
		}
		else {
			throw IOException("EqualityDeleteFile required property 'content' is missing");
		}

		auto equality_ids_val = yyjson_obj_get(obj, "equality-ids");
		if (equality_ids_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(equality_ids_val, idx, max, val) {
				result.equality_ids.push_back(yyjson_get_sint(val));
			}
		}

		return result;
	}

public:
	ContentFile content_file;
	string content;
	vector<int64_t> equality_ids;
};
} // namespace rest_api_objects
} // namespace duckdb