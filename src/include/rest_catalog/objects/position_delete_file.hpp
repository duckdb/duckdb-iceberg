
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/content_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PositionDeleteFile {
public:
	PositionDeleteFile() {
	}

public:
	static PositionDeleteFile FromJSON(yyjson_val *obj) {
		PositionDeleteFile res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
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

		auto content_offset_val = yyjson_obj_get(obj, "content_offset");
		if (content_offset_val) {
			content_offset = yyjson_get_sint(content_offset_val);
		}

		auto content_size_in_bytes_val = yyjson_obj_get(obj, "content_size_in_bytes");
		if (content_size_in_bytes_val) {
			content_size_in_bytes = yyjson_get_sint(content_size_in_bytes_val);
		}

		return string();
	}

public:
	ContentFile content_file;

public:
	string content;
	int64_t content_offset;
	int64_t content_size_in_bytes;
};

} // namespace rest_api_objects
} // namespace duckdb
