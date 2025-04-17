
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
		if (yyjson_is_int(spec_id_val)) {
			spec_id = yyjson_get_int(spec_id_val);
		} else {
			return StringUtil::Format("ContentFile property 'spec_id' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(spec_id_val));
		}
	}
	auto partition_val = yyjson_obj_get(obj, "partition");
	if (!partition_val) {
		return "ContentFile required property 'partition' is missing";
	} else {
		if (yyjson_is_arr(partition_val)) {
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
		} else {
			return StringUtil::Format("ContentFile property 'partition' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(partition_val));
		}
	}
	auto content_val = yyjson_obj_get(obj, "content");
	if (!content_val) {
		return "ContentFile required property 'content' is missing";
	} else {
		if (yyjson_is_str(content_val)) {
			content = yyjson_get_str(content_val);
		} else {
			return StringUtil::Format("ContentFile property 'content' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(content_val));
		}
	}
	auto file_path_val = yyjson_obj_get(obj, "file-path");
	if (!file_path_val) {
		return "ContentFile required property 'file-path' is missing";
	} else {
		if (yyjson_is_str(file_path_val)) {
			file_path = yyjson_get_str(file_path_val);
		} else {
			return StringUtil::Format("ContentFile property 'file_path' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(file_path_val));
		}
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
		if (yyjson_is_int(file_size_in_bytes_val)) {
			file_size_in_bytes = yyjson_get_int(file_size_in_bytes_val);
		} else {
			return StringUtil::Format(
			    "ContentFile property 'file_size_in_bytes' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(file_size_in_bytes_val));
		}
	}
	auto record_count_val = yyjson_obj_get(obj, "record-count");
	if (!record_count_val) {
		return "ContentFile required property 'record-count' is missing";
	} else {
		if (yyjson_is_int(record_count_val)) {
			record_count = yyjson_get_int(record_count_val);
		} else {
			return StringUtil::Format(
			    "ContentFile property 'record_count' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(record_count_val));
		}
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
		if (yyjson_is_arr(split_offsets_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(split_offsets_val, idx, max, val) {
				int64_t tmp;
				if (yyjson_is_int(val)) {
					tmp = yyjson_get_int(val);
				} else {
					return StringUtil::Format("ContentFile property 'tmp' is not of type 'integer', found '%s' instead",
					                          yyjson_get_type_desc(val));
				}
				split_offsets.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("ContentFile property 'split_offsets' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(split_offsets_val));
		}
	}
	auto sort_order_id_val = yyjson_obj_get(obj, "sort-order-id");
	if (sort_order_id_val) {
		has_sort_order_id = true;
		if (yyjson_is_int(sort_order_id_val)) {
			sort_order_id = yyjson_get_int(sort_order_id_val);
		} else {
			return StringUtil::Format(
			    "ContentFile property 'sort_order_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(sort_order_id_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
