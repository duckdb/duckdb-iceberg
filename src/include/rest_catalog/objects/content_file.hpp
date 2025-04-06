
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
		result.spec_id = yyjson_get_sint(spec_id_val);

		auto partition_val = yyjson_obj_get(obj, "partition");
		if (!partition_val) {
		return "ContentFile required property 'partition' is missing");
		}
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(partition_val, idx, max, val) {
			result.partition.push_back(PrimitiveTypeValue::FromJSON(val));
		}

		auto content_val = yyjson_obj_get(obj, "content");
		if (!content_val) {
		return "ContentFile required property 'content' is missing");
		}
		result.content = yyjson_get_str(content_val);

		auto file_path_val = yyjson_obj_get(obj, "file_path");
		if (!file_path_val) {
		return "ContentFile required property 'file_path' is missing");
		}
		result.file_path = yyjson_get_str(file_path_val);

		auto file_format_val = yyjson_obj_get(obj, "file_format");
		if (!file_format_val) {
		return "ContentFile required property 'file_format' is missing");
		}
		result.file_format = FileFormat::FromJSON(file_format_val);

		auto file_size_in_bytes_val = yyjson_obj_get(obj, "file_size_in_bytes");
		if (!file_size_in_bytes_val) {
		return "ContentFile required property 'file_size_in_bytes' is missing");
		}
		result.file_size_in_bytes = yyjson_get_sint(file_size_in_bytes_val);

		auto record_count_val = yyjson_obj_get(obj, "record_count");
		if (!record_count_val) {
		return "ContentFile required property 'record_count' is missing");
		}
		result.record_count = yyjson_get_sint(record_count_val);

		auto key_metadata_val = yyjson_obj_get(obj, "key_metadata");
		if (key_metadata_val) {
			result.key_metadata = key_metadata_val;
			;
		}

		auto split_offsets_val = yyjson_obj_get(obj, "split_offsets");
		if (split_offsets_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(split_offsets_val, idx, max, val) {
				result.split_offsets.push_back(yyjson_get_sint(val));
			};
		}

		auto sort_order_id_val = yyjson_obj_get(obj, "sort_order_id");
		if (sort_order_id_val) {
			result.sort_order_id = yyjson_get_sint(sort_order_id_val);
			;
		}
		return string();
	}

public:
public:
};

} // namespace rest_api_objects
} // namespace duckdb
