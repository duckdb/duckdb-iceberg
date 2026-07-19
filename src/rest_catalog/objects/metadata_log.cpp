
#include "rest_catalog/objects/metadata_log.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

MetadataLog::MetadataLog() {
}
MetadataLog::Object4::Object4() {
}

MetadataLog::Object4 MetadataLog::Object4::FromJSON(yyjson_val *obj) {
	Object4 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

MetadataLog::Object4 MetadataLog::Object4::Copy() const {
	Object4 res;
	res.metadata_file = metadata_file;
	res.timestamp_ms = timestamp_ms;
	return res;
}

string MetadataLog::Object4::TryFromJSON(yyjson_val *obj) {
	string error;
	auto metadata_file_val = yyjson_obj_get(obj, "metadata-file");
	if (!metadata_file_val) {
		return "Object4 required property 'metadata-file' is missing";
	} else {
		if (yyjson_is_str(metadata_file_val)) {
			metadata_file = yyjson_get_str(metadata_file_val);
		} else {
			return StringUtil::Format("Object4 property 'metadata_file' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(metadata_file_val));
		}
	}
	auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp-ms");
	if (!timestamp_ms_val) {
		return "Object4 required property 'timestamp-ms' is missing";
	} else {
		if (yyjson_is_sint(timestamp_ms_val)) {
			timestamp_ms = yyjson_get_sint(timestamp_ms_val);
		} else if (yyjson_is_uint(timestamp_ms_val)) {
			timestamp_ms = yyjson_get_uint(timestamp_ms_val);
		} else {
			return StringUtil::Format("Object4 property 'timestamp_ms' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(timestamp_ms_val));
		}
	}
	return "";
}

void MetadataLog::Object4::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: metadata-file
	yyjson_mut_obj_add_strcpy(doc, obj, "metadata-file", metadata_file.c_str());

	// Serialize: timestamp-ms
	yyjson_mut_obj_add_sint(doc, obj, "timestamp-ms", timestamp_ms);
}

yyjson_mut_val *MetadataLog::Object4::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

MetadataLog MetadataLog::FromJSON(yyjson_val *obj) {
	MetadataLog res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

MetadataLog MetadataLog::Copy() const {
	MetadataLog res;
	res.value.reserve(value.size());
	for (auto &item : value) {
		res.value.emplace_back(item.Copy());
	}
	return res;
}

string MetadataLog::TryFromJSON(yyjson_val *obj) {
	string error;
	if (yyjson_is_arr(obj)) {
		size_t value_idx, value_max;
		yyjson_val *value_item_val;
		yyjson_arr_foreach(obj, value_idx, value_max, value_item_val) {
			Object4 value_item;
			error = value_item.TryFromJSON(value_item_val);
			if (!error.empty()) {
				return error;
			}
			value.emplace_back(std::move(value_item));
		}
	} else {
		return StringUtil::Format("MetadataLog property 'value' is not of type 'array', found '%s' instead",
		                          yyjson_get_type_desc(obj));
	}
	return "";
}

yyjson_mut_val *MetadataLog::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *arr = yyjson_mut_arr(doc);
	for (const auto &item : value) {
		yyjson_mut_arr_append(arr, item.ToJSON(doc));
	}
	return arr;
}

} // namespace rest_api_objects
} // namespace duckdb
