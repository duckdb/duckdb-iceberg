
#include "rest_catalog/objects/content_file.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ContentFile::ContentFile() {
}

ContentFile ContentFile::FromJSON(yyjson_val *obj) {
	ContentFile res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string ContentFile::TryFromJSON(yyjson_val *obj) {
	string error;
	auto spec_id_val = yyjson_obj_get(obj, "spec-id");
	if (!spec_id_val) {
		return "ContentFile required property 'spec-id' is missing";
	} else {
		spec_id = yyjson_get_sint(spec_id_val);
	}
	auto partition_val = yyjson_obj_get(obj, "partition");
	if (!partition_val) {
		return "ContentFile required property 'partition' is missing";
	} else {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(partition_val, idx, max, val) {
			PrimitiveTypeValue tmp;
			error = tmp.TryFromJSON(val);
			if (!error.empty()) {
				return error;
			}
			partition.emplace_back(std::move(tmp));
		}
	}
	auto content_val = yyjson_obj_get(obj, "content");
	if (!content_val) {
		return "ContentFile required property 'content' is missing";
	} else {
		content = yyjson_get_str(content_val);
	}
	auto file_path_val = yyjson_obj_get(obj, "file-path");
	if (!file_path_val) {
		return "ContentFile required property 'file-path' is missing";
	} else {
		file_path = yyjson_get_str(file_path_val);
	}
	auto file_format_val = yyjson_obj_get(obj, "file-format");
	if (!file_format_val) {
		return "ContentFile required property 'file-format' is missing";
	} else {
		error = file_format.TryFromJSON(file_format_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto file_size_in_bytes_val = yyjson_obj_get(obj, "file-size-in-bytes");
	if (!file_size_in_bytes_val) {
		return "ContentFile required property 'file-size-in-bytes' is missing";
	} else {
		file_size_in_bytes = yyjson_get_sint(file_size_in_bytes_val);
	}
	auto record_count_val = yyjson_obj_get(obj, "record-count");
	if (!record_count_val) {
		return "ContentFile required property 'record-count' is missing";
	} else {
		record_count = yyjson_get_sint(record_count_val);
	}
	auto key_metadata_val = yyjson_obj_get(obj, "key-metadata");
	if (key_metadata_val) {
		has_key_metadata = true;
		error = key_metadata.TryFromJSON(key_metadata_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto split_offsets_val = yyjson_obj_get(obj, "split-offsets");
	if (split_offsets_val) {
		has_split_offsets = true;
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(split_offsets_val, idx, max, val) {
			auto tmp = yyjson_get_sint(val);
			split_offsets.emplace_back(std::move(tmp));
		}
	}
	auto sort_order_id_val = yyjson_obj_get(obj, "sort-order-id");
	if (sort_order_id_val) {
		has_sort_order_id = true;
		sort_order_id = yyjson_get_sint(sort_order_id_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
