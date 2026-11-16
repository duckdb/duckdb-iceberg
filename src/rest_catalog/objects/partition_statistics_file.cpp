
#include "rest_catalog/objects/partition_statistics_file.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

PartitionStatisticsFile::PartitionStatisticsFile() {
}

PartitionStatisticsFile PartitionStatisticsFile::FromJSON(JSONValue obj) {
	PartitionStatisticsFile res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

PartitionStatisticsFile PartitionStatisticsFile::Copy() const {
	PartitionStatisticsFile res;
	res.snapshot_id = snapshot_id;
	res.statistics_path = statistics_path;
	res.file_size_in_bytes = file_size_in_bytes;
	return res;
}

string PartitionStatisticsFile::TryFromJSON(JSONValue obj) {
	string error;
	auto snapshot_id_val = obj.GetMember("snapshot-id");
	if (!snapshot_id_val.IsValid()) {
		return "PartitionStatisticsFile required property 'snapshot-id' is missing";
	} else {
		if (json_utils::IsInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetSignedInteger(snapshot_id_val);
		} else if (json_utils::IsUnsignedInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetUnsignedInteger(snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "PartitionStatisticsFile property 'snapshot_id' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(snapshot_id_val).c_str());
		}
	}
	auto statistics_path_val = obj.GetMember("statistics-path");
	if (!statistics_path_val.IsValid()) {
		return "PartitionStatisticsFile required property 'statistics-path' is missing";
	} else {
		if (json_utils::IsString(statistics_path_val)) {
			statistics_path = json_utils::GetString(statistics_path_val);
		} else {
			return StringUtil::Format(
			    "PartitionStatisticsFile property 'statistics_path' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(statistics_path_val).c_str());
		}
	}
	auto file_size_in_bytes_val = obj.GetMember("file-size-in-bytes");
	if (!file_size_in_bytes_val.IsValid()) {
		return "PartitionStatisticsFile required property 'file-size-in-bytes' is missing";
	} else {
		if (json_utils::IsInteger(file_size_in_bytes_val)) {
			file_size_in_bytes = json_utils::GetSignedInteger(file_size_in_bytes_val);
		} else if (json_utils::IsUnsignedInteger(file_size_in_bytes_val)) {
			file_size_in_bytes = json_utils::GetUnsignedInteger(file_size_in_bytes_val);
		} else {
			return StringUtil::Format(
			    "PartitionStatisticsFile property 'file_size_in_bytes' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(file_size_in_bytes_val).c_str());
		}
	}
	return "";
}

void PartitionStatisticsFile::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: snapshot-id
	auto snapshot_id_json = writer.CreateSignedInteger(snapshot_id);
	obj.Add("snapshot-id", snapshot_id_json);

	// Serialize: statistics-path
	auto statistics_path_json = writer.CreateString(statistics_path);
	obj.Add("statistics-path", statistics_path_json);

	// Serialize: file-size-in-bytes
	auto file_size_in_bytes_json = writer.CreateSignedInteger(file_size_in_bytes);
	obj.Add("file-size-in-bytes", file_size_in_bytes_json);
}

JSONMutableValue PartitionStatisticsFile::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
