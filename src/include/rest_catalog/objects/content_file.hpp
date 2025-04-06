
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/file_format.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ContentFile {
public:
	ContentFile::ContentFile() {
	}

public:
	static ContentFile FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto spec_id_val = yyjson_obj_get(obj, "spec_id");
		if (!spec_id_val) {
		return "ContentFile required property 'spec_id' is missing");
		}
		spec_id = yyjson_get_sint(spec_id_val);

		auto partition_val = yyjson_obj_get(obj, "partition");
		if (!partition_val) {
		return "ContentFile required property 'partition' is missing");
		}
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(partition_val, idx, max, val) {

			PrimitiveTypeValue tmp;
			error = tmp.TryFromJSON(val);
			if (!error.empty()) {
				return error;
			}
			partition.push_back(tmp);
		}

		auto content_val = yyjson_obj_get(obj, "content");
		if (!content_val) {
		return "ContentFile required property 'content' is missing");
		}
		content = yyjson_get_str(content_val);

		auto file_path_val = yyjson_obj_get(obj, "file_path");
		if (!file_path_val) {
		return "ContentFile required property 'file_path' is missing");
		}
		file_path = yyjson_get_str(file_path_val);

		auto file_format_val = yyjson_obj_get(obj, "file_format");
		if (!file_format_val) {
		return "ContentFile required property 'file_format' is missing");
		}
		error = file_format.TryFromJSON(file_format_val);
		if (!error.empty()) {
			return error;
		}

		auto file_size_in_bytes_val = yyjson_obj_get(obj, "file_size_in_bytes");
		if (!file_size_in_bytes_val) {
		return "ContentFile required property 'file_size_in_bytes' is missing");
		}
		file_size_in_bytes = yyjson_get_sint(file_size_in_bytes_val);

		auto record_count_val = yyjson_obj_get(obj, "record_count");
		if (!record_count_val) {
		return "ContentFile required property 'record_count' is missing");
		}
		record_count = yyjson_get_sint(record_count_val);

		auto key_metadata_val = yyjson_obj_get(obj, "key_metadata");
		if (key_metadata_val) {
			key_metadata = key_metadata_val;
		}

		auto split_offsets_val = yyjson_obj_get(obj, "split_offsets");
		if (split_offsets_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(split_offsets_val, idx, max, val) {

				auto tmp = yyjson_get_sint(val);
				split_offsets.push_back(tmp);
			}
		}

		auto sort_order_id_val = yyjson_obj_get(obj, "sort_order_id");
		if (sort_order_id_val) {
			sort_order_id = yyjson_get_sint(sort_order_id_val);
		}

		return string();
	}

public:
public:
	string content;
	FileFormat file_format;
	string file_path;
	int64_t file_size_in_bytes;
	yyjson_val *key_metadata;
	vector<PrimitiveTypeValue> partition;
	int64_t record_count;
	int64_t sort_order_id;
	int64_t spec_id;
	vector<int64_t> split_offsets;
};

} // namespace rest_api_objects
} // namespace duckdb
