
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
		snapshot_id = yyjson_get_sint(snapshot_id_val);
	}
	auto statistics_path_val = yyjson_obj_get(obj, "statistics-path");
	if (!statistics_path_val) {
		return "PartitionStatisticsFile required property 'statistics-path' is missing";
	} else {
		statistics_path = yyjson_get_str(statistics_path_val);
	}
	auto file_size_in_bytes_val = yyjson_obj_get(obj, "file-size-in-bytes");
	if (!file_size_in_bytes_val) {
		return "PartitionStatisticsFile required property 'file-size-in-bytes' is missing";
	} else {
		file_size_in_bytes = yyjson_get_sint(file_size_in_bytes_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
