
#include "rest_catalog/objects/statistics_file.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

StatisticsFile::StatisticsFile() {
}

StatisticsFile StatisticsFile::FromJSON(yyjson_val *obj) {
	StatisticsFile res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

StatisticsFile StatisticsFile::Copy() const {
	StatisticsFile res;
	res.snapshot_id = snapshot_id;
	res.statistics_path = statistics_path;
	res.file_size_in_bytes = file_size_in_bytes;
	res.file_footer_size_in_bytes = file_footer_size_in_bytes;
	res.blob_metadata.reserve(blob_metadata.size());
	for (auto &item : blob_metadata) {
		res.blob_metadata.emplace_back(item.Copy());
	}
	return res;
}

string StatisticsFile::TryFromJSON(yyjson_val *obj) {
	string error;
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "StatisticsFile required property 'snapshot-id' is missing";
	} else {
		if (yyjson_is_sint(snapshot_id_val)) {
			snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else if (yyjson_is_uint(snapshot_id_val)) {
			snapshot_id = yyjson_get_uint(snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "StatisticsFile property 'snapshot_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(snapshot_id_val));
		}
	}
	auto statistics_path_val = yyjson_obj_get(obj, "statistics-path");
	if (!statistics_path_val) {
		return "StatisticsFile required property 'statistics-path' is missing";
	} else {
		if (yyjson_is_str(statistics_path_val)) {
			statistics_path = yyjson_get_str(statistics_path_val);
		} else {
			return StringUtil::Format(
			    "StatisticsFile property 'statistics_path' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(statistics_path_val));
		}
	}
	auto file_size_in_bytes_val = yyjson_obj_get(obj, "file-size-in-bytes");
	if (!file_size_in_bytes_val) {
		return "StatisticsFile required property 'file-size-in-bytes' is missing";
	} else {
		if (yyjson_is_sint(file_size_in_bytes_val)) {
			file_size_in_bytes = yyjson_get_sint(file_size_in_bytes_val);
		} else if (yyjson_is_uint(file_size_in_bytes_val)) {
			file_size_in_bytes = yyjson_get_uint(file_size_in_bytes_val);
		} else {
			return StringUtil::Format(
			    "StatisticsFile property 'file_size_in_bytes' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(file_size_in_bytes_val));
		}
	}
	auto file_footer_size_in_bytes_val = yyjson_obj_get(obj, "file-footer-size-in-bytes");
	if (!file_footer_size_in_bytes_val) {
		return "StatisticsFile required property 'file-footer-size-in-bytes' is missing";
	} else {
		if (yyjson_is_sint(file_footer_size_in_bytes_val)) {
			file_footer_size_in_bytes = yyjson_get_sint(file_footer_size_in_bytes_val);
		} else if (yyjson_is_uint(file_footer_size_in_bytes_val)) {
			file_footer_size_in_bytes = yyjson_get_uint(file_footer_size_in_bytes_val);
		} else {
			return StringUtil::Format(
			    "StatisticsFile property 'file_footer_size_in_bytes' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(file_footer_size_in_bytes_val));
		}
	}
	auto blob_metadata_val = yyjson_obj_get(obj, "blob-metadata");
	if (!blob_metadata_val) {
		return "StatisticsFile required property 'blob-metadata' is missing";
	} else {
		if (yyjson_is_arr(blob_metadata_val)) {
			size_t blob_metadata_idx, blob_metadata_max;
			yyjson_val *blob_metadata_item_val;
			yyjson_arr_foreach(blob_metadata_val, blob_metadata_idx, blob_metadata_max, blob_metadata_item_val) {
				BlobMetadata blob_metadata_item;
				error = blob_metadata_item.TryFromJSON(blob_metadata_item_val);
				if (!error.empty()) {
					return error;
				}
				blob_metadata.emplace_back(std::move(blob_metadata_item));
			}
		} else {
			return StringUtil::Format(
			    "StatisticsFile property 'blob_metadata' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(blob_metadata_val));
		}
	}
	return "";
}

void StatisticsFile::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: snapshot-id
	yyjson_mut_obj_add_sint(doc, obj, "snapshot-id", snapshot_id);

	// Serialize: statistics-path
	yyjson_mut_obj_add_strcpy(doc, obj, "statistics-path", statistics_path.c_str());

	// Serialize: file-size-in-bytes
	yyjson_mut_obj_add_sint(doc, obj, "file-size-in-bytes", file_size_in_bytes);

	// Serialize: file-footer-size-in-bytes
	yyjson_mut_obj_add_sint(doc, obj, "file-footer-size-in-bytes", file_footer_size_in_bytes);

	// Serialize: blob-metadata
	yyjson_mut_val *blob_metadata_arr = yyjson_mut_arr(doc);
	for (const auto &item : blob_metadata) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(blob_metadata_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "blob-metadata", blob_metadata_arr);
}

yyjson_mut_val *StatisticsFile::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
