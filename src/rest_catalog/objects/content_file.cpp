
#include "rest_catalog/objects/content_file.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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

ContentFile ContentFile::Copy() const {
	ContentFile res;
	res.spec_id = spec_id;
	res.partition.reserve(partition.size());
	for (auto &item : partition) {
		res.partition.emplace_back(item.Copy());
	}
	res.content = content;
	res.file_path = file_path;
	res.file_format = file_format.Copy();
	res.file_size_in_bytes = file_size_in_bytes;
	res.record_count = record_count;
	if (key_metadata.has_value()) {
		res.key_metadata.emplace();
		(*res.key_metadata) = (*key_metadata).Copy();
	}
	if (split_offsets.has_value()) {
		res.split_offsets.emplace();
		(*res.split_offsets).reserve((*split_offsets).size());
		for (auto &item : (*split_offsets)) {
			(*res.split_offsets).emplace_back(item);
		}
	}
	if (sort_order_id.has_value()) {
		res.sort_order_id.emplace();
		(*res.sort_order_id) = (*sort_order_id);
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
			size_t partition_idx, partition_max;
			yyjson_val *partition_item_val;
			yyjson_arr_foreach(partition_val, partition_idx, partition_max, partition_item_val) {
				PrimitiveTypeValue partition_item;
				error = partition_item.TryFromJSON(partition_item_val);
				if (!error.empty()) {
					return error;
				}
				partition.emplace_back(std::move(partition_item));
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
		if (yyjson_is_sint(file_size_in_bytes_val)) {
			file_size_in_bytes = yyjson_get_sint(file_size_in_bytes_val);
		} else if (yyjson_is_uint(file_size_in_bytes_val)) {
			file_size_in_bytes = yyjson_get_uint(file_size_in_bytes_val);
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
		if (yyjson_is_sint(record_count_val)) {
			record_count = yyjson_get_sint(record_count_val);
		} else if (yyjson_is_uint(record_count_val)) {
			record_count = yyjson_get_uint(record_count_val);
		} else {
			return StringUtil::Format(
			    "ContentFile property 'record_count' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(record_count_val));
		}
	}
	auto key_metadata_val = yyjson_obj_get(obj, "key-metadata");
	if (key_metadata_val) {
		BinaryTypeValue key_metadata_tmp;
		error = key_metadata_tmp.TryFromJSON(key_metadata_val);
		if (!error.empty()) {
			return error;
		}
		key_metadata = std::move(key_metadata_tmp);
	}
	auto split_offsets_val = yyjson_obj_get(obj, "split-offsets");
	if (split_offsets_val) {
		vector<int64_t> split_offsets_tmp;
		if (yyjson_is_arr(split_offsets_val)) {
			size_t split_offsets_tmp_idx, split_offsets_tmp_max;
			yyjson_val *split_offsets_tmp_item_val;
			yyjson_arr_foreach(split_offsets_val, split_offsets_tmp_idx, split_offsets_tmp_max,
			                   split_offsets_tmp_item_val) {
				int64_t split_offsets_tmp_item;
				if (yyjson_is_sint(split_offsets_tmp_item_val)) {
					split_offsets_tmp_item = yyjson_get_sint(split_offsets_tmp_item_val);
				} else if (yyjson_is_uint(split_offsets_tmp_item_val)) {
					split_offsets_tmp_item = yyjson_get_uint(split_offsets_tmp_item_val);
				} else {
					return StringUtil::Format(
					    "ContentFile property 'split_offsets_tmp_item' is not of type 'integer', found '%s' instead",
					    yyjson_get_type_desc(split_offsets_tmp_item_val));
				}
				split_offsets_tmp.emplace_back(std::move(split_offsets_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "ContentFile property 'split_offsets_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(split_offsets_val));
		}
		split_offsets = std::move(split_offsets_tmp);
	}
	auto sort_order_id_val = yyjson_obj_get(obj, "sort-order-id");
	if (sort_order_id_val) {
		int32_t sort_order_id_tmp;
		if (yyjson_is_int(sort_order_id_val)) {
			sort_order_id_tmp = yyjson_get_int(sort_order_id_val);
		} else {
			return StringUtil::Format(
			    "ContentFile property 'sort_order_id_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(sort_order_id_val));
		}
		sort_order_id = std::move(sort_order_id_tmp);
	}
	return "";
}

void ContentFile::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: spec-id
	yyjson_mut_obj_add_int(doc, obj, "spec-id", spec_id);

	// Serialize: partition
	yyjson_mut_val *partition_arr = yyjson_mut_arr(doc);
	for (const auto &item : partition) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(partition_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "partition", partition_arr);

	// Serialize: content
	yyjson_mut_obj_add_strcpy(doc, obj, "content", content.c_str());

	// Serialize: file-path
	yyjson_mut_obj_add_strcpy(doc, obj, "file-path", file_path.c_str());

	// Serialize: file-format
	yyjson_mut_val *file_format_val = file_format.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "file-format", file_format_val);

	// Serialize: file-size-in-bytes
	yyjson_mut_obj_add_sint(doc, obj, "file-size-in-bytes", file_size_in_bytes);

	// Serialize: record-count
	yyjson_mut_obj_add_sint(doc, obj, "record-count", record_count);

	// Serialize: key-metadata
	if (key_metadata.has_value()) {
		auto &key_metadata_value = *key_metadata;
		yyjson_mut_val *key_metadata_value_val = key_metadata_value.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "key-metadata", key_metadata_value_val);
	}

	// Serialize: split-offsets
	if (split_offsets.has_value()) {
		auto &split_offsets_value = *split_offsets;
		yyjson_mut_val *split_offsets_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : split_offsets_value) {
			yyjson_mut_val *item_val = yyjson_mut_sint(doc, item);
			yyjson_mut_arr_append(split_offsets_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "split-offsets", split_offsets_value_arr);
	}

	// Serialize: sort-order-id
	if (sort_order_id.has_value()) {
		auto &sort_order_id_value = *sort_order_id;
		yyjson_mut_obj_add_int(doc, obj, "sort-order-id", sort_order_id_value);
	}
}

yyjson_mut_val *ContentFile::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
