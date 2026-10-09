
#include "rest_catalog/objects/content_file.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ContentFile::ContentFile() {
}
ContentFile::Object9::Object9() {
}

ContentFile::Object9 ContentFile::Object9::FromJSON(JSONValue obj) {
	Object9 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ContentFile::Object9 ContentFile::Object9::Copy() const {
	Object9 res;
	for (auto &entry : additional_properties) {
		res.additional_properties.emplace(entry.first, entry.second.Copy());
	}
	return res;
}

string ContentFile::Object9::TryFromJSON(JSONValue obj) {
	string error;
	obj.IterateObject([&](const string &key_str, JSONValue val) {
		if (!error.empty()) {
			return;
		}
		FieldStatistics tmp;
		error = tmp.TryFromJSON(val);
		if (!error.empty()) {
			return;
		}
		additional_properties.emplace(key_str, std::move(tmp));
	});
	if (!error.empty()) {
		return error;
	}
	return "";
}

void ContentFile::Object9::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize additional properties
	for (const auto &[key, value] : additional_properties) {
		auto value_json = value.ToJSON(writer);
		obj.Add(key, value_json);
	}
}

JSONMutableValue ContentFile::Object9::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

ContentFile ContentFile::FromJSON(JSONValue obj) {
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
	if (content_stats.has_value()) {
		res.content_stats.emplace();
		(*res.content_stats) = (*content_stats).Copy();
	}
	return res;
}

string ContentFile::TryFromJSON(JSONValue obj) {
	string error;
	auto spec_id_val = obj.GetMember("spec-id");
	if (!spec_id_val.IsValid()) {
		return "ContentFile required property 'spec-id' is missing";
	} else {
		if (json_utils::IsInteger(spec_id_val)) {
			spec_id = json_utils::GetSignedInteger(spec_id_val);
		} else {
			return StringUtil::Format("ContentFile property 'spec_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(spec_id_val).c_str());
		}
	}
	auto partition_val = obj.GetMember("partition");
	if (!partition_val.IsValid()) {
		return "ContentFile required property 'partition' is missing";
	} else {
		if (partition_val.IsArray()) {
			partition_val.IterateArray([&](JSONValue partition_item_val) {
				if (!error.empty()) {
					return;
				}
				PrimitiveTypeValue partition_item;
				error = partition_item.TryFromJSON(partition_item_val);
				if (!error.empty()) {
					return;
				}
				partition.emplace_back(std::move(partition_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("ContentFile property 'partition' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(partition_val).c_str());
		}
	}
	auto content_val = obj.GetMember("content");
	if (!content_val.IsValid()) {
		return "ContentFile required property 'content' is missing";
	} else {
		if (json_utils::IsString(content_val)) {
			content = json_utils::GetString(content_val);
		} else {
			return StringUtil::Format("ContentFile property 'content' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(content_val).c_str());
		}
	}
	auto file_path_val = obj.GetMember("file-path");
	if (!file_path_val.IsValid()) {
		return "ContentFile required property 'file-path' is missing";
	} else {
		if (json_utils::IsString(file_path_val)) {
			file_path = json_utils::GetString(file_path_val);
		} else {
			return StringUtil::Format("ContentFile property 'file_path' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(file_path_val).c_str());
		}
	}
	auto file_format_val = obj.GetMember("file-format");
	if (!file_format_val.IsValid()) {
		return "ContentFile required property 'file-format' is missing";
	} else {
		error = file_format.TryFromJSON(file_format_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto file_size_in_bytes_val = obj.GetMember("file-size-in-bytes");
	if (!file_size_in_bytes_val.IsValid()) {
		return "ContentFile required property 'file-size-in-bytes' is missing";
	} else {
		if (json_utils::IsInteger(file_size_in_bytes_val)) {
			file_size_in_bytes = json_utils::GetSignedInteger(file_size_in_bytes_val);
		} else if (json_utils::IsUnsignedInteger(file_size_in_bytes_val)) {
			file_size_in_bytes = json_utils::GetUnsignedInteger(file_size_in_bytes_val);
		} else {
			return StringUtil::Format(
			    "ContentFile property 'file_size_in_bytes' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(file_size_in_bytes_val).c_str());
		}
	}
	auto record_count_val = obj.GetMember("record-count");
	if (!record_count_val.IsValid()) {
		return "ContentFile required property 'record-count' is missing";
	} else {
		if (json_utils::IsInteger(record_count_val)) {
			record_count = json_utils::GetSignedInteger(record_count_val);
		} else if (json_utils::IsUnsignedInteger(record_count_val)) {
			record_count = json_utils::GetUnsignedInteger(record_count_val);
		} else {
			return StringUtil::Format("ContentFile property 'record_count' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(record_count_val).c_str());
		}
	}
	auto key_metadata_val = obj.GetMember("key-metadata");
	if (key_metadata_val.IsValid()) {
		BinaryTypeValue key_metadata_tmp;
		error = key_metadata_tmp.TryFromJSON(key_metadata_val);
		if (!error.empty()) {
			return error;
		}
		key_metadata = std::move(key_metadata_tmp);
	}
	auto split_offsets_val = obj.GetMember("split-offsets");
	if (split_offsets_val.IsValid()) {
		vector<int64_t> split_offsets_tmp;
		if (split_offsets_val.IsArray()) {
			split_offsets_val.IterateArray([&](JSONValue split_offsets_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				int64_t split_offsets_tmp_item;
				if (json_utils::IsInteger(split_offsets_tmp_item_val)) {
					split_offsets_tmp_item = json_utils::GetSignedInteger(split_offsets_tmp_item_val);
				} else if (json_utils::IsUnsignedInteger(split_offsets_tmp_item_val)) {
					split_offsets_tmp_item = json_utils::GetUnsignedInteger(split_offsets_tmp_item_val);
				} else {
					error = StringUtil::Format(
					    "ContentFile property 'split_offsets_tmp_item' is not of type 'integer', found %s instead",
					    json_utils::GetTypeDescription(split_offsets_tmp_item_val).c_str());
					return;
				}
				split_offsets_tmp.emplace_back(std::move(split_offsets_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "ContentFile property 'split_offsets_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(split_offsets_val).c_str());
		}
		split_offsets = std::move(split_offsets_tmp);
	}
	auto sort_order_id_val = obj.GetMember("sort-order-id");
	if (sort_order_id_val.IsValid()) {
		int32_t sort_order_id_tmp;
		if (json_utils::IsInteger(sort_order_id_val)) {
			sort_order_id_tmp = json_utils::GetSignedInteger(sort_order_id_val);
		} else {
			return StringUtil::Format(
			    "ContentFile property 'sort_order_id_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(sort_order_id_val).c_str());
		}
		sort_order_id = std::move(sort_order_id_tmp);
	}
	auto content_stats_val = obj.GetMember("content-stats");
	if (content_stats_val.IsValid()) {
		Object9 content_stats_tmp;
		error = content_stats_tmp.TryFromJSON(content_stats_val);
		if (!error.empty()) {
			return error;
		}
		content_stats = std::move(content_stats_tmp);
	}
	return "";
}

void ContentFile::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: spec-id
	auto spec_id_json = writer.CreateSignedInteger(spec_id);
	obj.Add("spec-id", spec_id_json);

	// Serialize: partition
	auto partition_json = writer.CreateArray();
	for (const auto &partition_json_item : partition) {
		auto partition_json_item_json = partition_json_item.ToJSON(writer);
		partition_json.Append(partition_json_item_json);
	}
	obj.Add("partition", partition_json);

	// Serialize: content
	auto content_json = writer.CreateString(content);
	obj.Add("content", content_json);

	// Serialize: file-path
	auto file_path_json = writer.CreateString(file_path);
	obj.Add("file-path", file_path_json);

	// Serialize: file-format
	auto file_format_json = file_format.ToJSON(writer);
	obj.Add("file-format", file_format_json);

	// Serialize: file-size-in-bytes
	auto file_size_in_bytes_json = writer.CreateSignedInteger(file_size_in_bytes);
	obj.Add("file-size-in-bytes", file_size_in_bytes_json);

	// Serialize: record-count
	auto record_count_json = writer.CreateSignedInteger(record_count);
	obj.Add("record-count", record_count_json);

	// Serialize: key-metadata
	if (key_metadata.has_value()) {
		auto &key_metadata_value = *key_metadata;
		auto key_metadata_json = key_metadata_value.ToJSON(writer);
		obj.Add("key-metadata", key_metadata_json);
	}

	// Serialize: split-offsets
	if (split_offsets.has_value()) {
		auto &split_offsets_value = *split_offsets;
		auto split_offsets_json = writer.CreateArray();
		for (const auto &split_offsets_json_item : split_offsets_value) {
			auto split_offsets_json_item_json = writer.CreateSignedInteger(split_offsets_json_item);
			split_offsets_json.Append(split_offsets_json_item_json);
		}
		obj.Add("split-offsets", split_offsets_json);
	}

	// Serialize: sort-order-id
	if (sort_order_id.has_value()) {
		auto &sort_order_id_value = *sort_order_id;
		auto sort_order_id_json = writer.CreateSignedInteger(sort_order_id_value);
		obj.Add("sort-order-id", sort_order_id_json);
	}

	// Serialize: content-stats
	if (content_stats.has_value()) {
		auto &content_stats_value = *content_stats;
		auto content_stats_json = content_stats_value.ToJSON(writer);
		obj.Add("content-stats", content_stats_json);
	}
}

JSONMutableValue ContentFile::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
