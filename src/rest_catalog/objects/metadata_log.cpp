
#include "rest_catalog/objects/metadata_log.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

MetadataLog::MetadataLog() {
}
MetadataLog::Object8::Object8() {
}

MetadataLog::Object8 MetadataLog::Object8::FromJSON(JSONValue obj) {
	Object8 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

MetadataLog::Object8 MetadataLog::Object8::Copy() const {
	Object8 res;
	res.metadata_file = metadata_file;
	res.timestamp_ms = timestamp_ms;
	return res;
}

string MetadataLog::Object8::TryFromJSON(JSONValue obj) {
	string error;
	auto metadata_file_val = obj.GetMember("metadata-file");
	if (!metadata_file_val.IsValid()) {
		return "Object8 required property 'metadata-file' is missing";
	} else {
		if (json_utils::IsString(metadata_file_val)) {
			metadata_file = json_utils::GetString(metadata_file_val);
		} else {
			return StringUtil::Format("Object8 property 'metadata_file' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(metadata_file_val).c_str());
		}
	}
	auto timestamp_ms_val = obj.GetMember("timestamp-ms");
	if (!timestamp_ms_val.IsValid()) {
		return "Object8 required property 'timestamp-ms' is missing";
	} else {
		if (json_utils::IsInteger(timestamp_ms_val)) {
			timestamp_ms = json_utils::GetSignedInteger(timestamp_ms_val);
		} else if (json_utils::IsUnsignedInteger(timestamp_ms_val)) {
			timestamp_ms = json_utils::GetUnsignedInteger(timestamp_ms_val);
		} else {
			return StringUtil::Format("Object8 property 'timestamp_ms' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(timestamp_ms_val).c_str());
		}
	}
	return "";
}

void MetadataLog::Object8::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: metadata-file
	auto metadata_file_json = writer.CreateString(metadata_file);
	obj.Add("metadata-file", metadata_file_json);

	// Serialize: timestamp-ms
	auto timestamp_ms_json = writer.CreateSignedInteger(timestamp_ms);
	obj.Add("timestamp-ms", timestamp_ms_json);
}

JSONMutableValue MetadataLog::Object8::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

MetadataLog MetadataLog::FromJSON(JSONValue obj) {
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

string MetadataLog::TryFromJSON(JSONValue obj) {
	string error;
	if (obj.IsArray()) {
		obj.IterateArray([&](JSONValue value_item_val) {
			if (!error.empty()) {
				return;
			}
			Object8 value_item;
			error = value_item.TryFromJSON(value_item_val);
			if (!error.empty()) {
				return;
			}
			value.emplace_back(std::move(value_item));
		});
		if (!error.empty()) {
			return error;
		}
	} else {
		return StringUtil::Format("MetadataLog property 'value' is not of type 'array', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	return "";
}

JSONMutableValue MetadataLog::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateArray();
	for (const auto &result_item : value) {
		auto result_item_json = result_item.ToJSON(writer);
		result.Append(result_item_json);
	}
	return result;
}

} // namespace rest_api_objects
} // namespace duckdb
