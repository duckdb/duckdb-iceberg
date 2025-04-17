
#include "rest_catalog/objects/partition_statistics_file.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

PartitionStatisticsFile::PartitionStatisticsFile() {
}

PartitionStatisticsFile PartitionStatisticsFile::FromJSON(yyjson_val *obj) {
	PartitionStatisticsFile res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string PartitionStatisticsFile::TryFromJSON(yyjson_val *obj) {
	string error;
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "PartitionStatisticsFile required property 'snapshot-id' is missing";
	} else {
		if (yyjson_is_int(snapshot_id_val)) {
			snapshot_id = yyjson_get_int(snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "PartitionStatisticsFile property 'snapshot_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(snapshot_id_val));
		}
	}
	auto statistics_path_val = yyjson_obj_get(obj, "statistics-path");
	if (!statistics_path_val) {
		return "PartitionStatisticsFile required property 'statistics-path' is missing";
	} else {
		if (yyjson_is_str(statistics_path_val)) {
			statistics_path = yyjson_get_str(statistics_path_val);
		} else {
			return StringUtil::Format(
			    "PartitionStatisticsFile property 'statistics_path' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(statistics_path_val));
		}
	}
	auto file_size_in_bytes_val = yyjson_obj_get(obj, "file-size-in-bytes");
	if (!file_size_in_bytes_val) {
		return "PartitionStatisticsFile required property 'file-size-in-bytes' is missing";
	} else {
		if (yyjson_is_int(file_size_in_bytes_val)) {
			file_size_in_bytes = yyjson_get_int(file_size_in_bytes_val);
		} else {
			return StringUtil::Format(
			    "PartitionStatisticsFile property 'file_size_in_bytes' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(file_size_in_bytes_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
