
#include "rest_catalog/objects/statistics_file.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

StatisticsFile::StatisticsFile() {
}

StatisticsFile StatisticsFile::FromJSON(JSONValue obj) {
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

string StatisticsFile::TryFromJSON(JSONValue obj) {
	string error;
	auto snapshot_id_val = obj.GetMember("snapshot-id");
	if (!snapshot_id_val.IsValid()) {
		return "StatisticsFile required property 'snapshot-id' is missing";
	} else {
		if (json_utils::IsInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetSignedInteger(snapshot_id_val);
		} else if (json_utils::IsUnsignedInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetUnsignedInteger(snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "StatisticsFile property 'snapshot_id' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(snapshot_id_val).c_str());
		}
	}
	auto statistics_path_val = obj.GetMember("statistics-path");
	if (!statistics_path_val.IsValid()) {
		return "StatisticsFile required property 'statistics-path' is missing";
	} else {
		if (json_utils::IsString(statistics_path_val)) {
			statistics_path = json_utils::GetString(statistics_path_val);
		} else {
			return StringUtil::Format(
			    "StatisticsFile property 'statistics_path' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(statistics_path_val).c_str());
		}
	}
	auto file_size_in_bytes_val = obj.GetMember("file-size-in-bytes");
	if (!file_size_in_bytes_val.IsValid()) {
		return "StatisticsFile required property 'file-size-in-bytes' is missing";
	} else {
		if (json_utils::IsInteger(file_size_in_bytes_val)) {
			file_size_in_bytes = json_utils::GetSignedInteger(file_size_in_bytes_val);
		} else if (json_utils::IsUnsignedInteger(file_size_in_bytes_val)) {
			file_size_in_bytes = json_utils::GetUnsignedInteger(file_size_in_bytes_val);
		} else {
			return StringUtil::Format(
			    "StatisticsFile property 'file_size_in_bytes' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(file_size_in_bytes_val).c_str());
		}
	}
	auto file_footer_size_in_bytes_val = obj.GetMember("file-footer-size-in-bytes");
	if (!file_footer_size_in_bytes_val.IsValid()) {
		return "StatisticsFile required property 'file-footer-size-in-bytes' is missing";
	} else {
		if (json_utils::IsInteger(file_footer_size_in_bytes_val)) {
			file_footer_size_in_bytes = json_utils::GetSignedInteger(file_footer_size_in_bytes_val);
		} else if (json_utils::IsUnsignedInteger(file_footer_size_in_bytes_val)) {
			file_footer_size_in_bytes = json_utils::GetUnsignedInteger(file_footer_size_in_bytes_val);
		} else {
			return StringUtil::Format(
			    "StatisticsFile property 'file_footer_size_in_bytes' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(file_footer_size_in_bytes_val).c_str());
		}
	}
	auto blob_metadata_val = obj.GetMember("blob-metadata");
	if (!blob_metadata_val.IsValid()) {
		return "StatisticsFile required property 'blob-metadata' is missing";
	} else {
		if (blob_metadata_val.IsArray()) {
			blob_metadata_val.IterateArray([&](JSONValue blob_metadata_item_val) {
				if (!error.empty()) {
					return;
				}
				BlobMetadata blob_metadata_item;
				error = blob_metadata_item.TryFromJSON(blob_metadata_item_val);
				if (!error.empty()) {
					return;
				}
				blob_metadata.emplace_back(std::move(blob_metadata_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "StatisticsFile property 'blob_metadata' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(blob_metadata_val).c_str());
		}
	}
	return "";
}

void StatisticsFile::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: snapshot-id
	auto snapshot_id_json = writer.CreateSignedInteger(snapshot_id);
	obj.Add("snapshot-id", snapshot_id_json);

	// Serialize: statistics-path
	auto statistics_path_json = writer.CreateString(statistics_path);
	obj.Add("statistics-path", statistics_path_json);

	// Serialize: file-size-in-bytes
	auto file_size_in_bytes_json = writer.CreateSignedInteger(file_size_in_bytes);
	obj.Add("file-size-in-bytes", file_size_in_bytes_json);

	// Serialize: file-footer-size-in-bytes
	auto file_footer_size_in_bytes_json = writer.CreateSignedInteger(file_footer_size_in_bytes);
	obj.Add("file-footer-size-in-bytes", file_footer_size_in_bytes_json);

	// Serialize: blob-metadata
	auto blob_metadata_json = writer.CreateArray();
	for (const auto &blob_metadata_json_item : blob_metadata) {
		auto blob_metadata_json_item_json = blob_metadata_json_item.ToJSON(writer);
		blob_metadata_json.Append(blob_metadata_json_item_json);
	}
	obj.Add("blob-metadata", blob_metadata_json);
}

JSONMutableValue StatisticsFile::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
