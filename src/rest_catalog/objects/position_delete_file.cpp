
#include "rest_catalog/objects/position_delete_file.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

PositionDeleteFile::PositionDeleteFile() {
}

PositionDeleteFile PositionDeleteFile::FromJSON(JSONValue obj) {
	PositionDeleteFile res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

PositionDeleteFile PositionDeleteFile::Copy() const {
	PositionDeleteFile res;
	res.content_file = content_file.Copy();
	if (content_offset.has_value()) {
		res.content_offset.emplace();
		(*res.content_offset) = (*content_offset);
	}
	if (content_size_in_bytes.has_value()) {
		res.content_size_in_bytes.emplace();
		(*res.content_size_in_bytes) = (*content_size_in_bytes);
	}
	return res;
}

string PositionDeleteFile::TryFromJSON(JSONValue obj) {
	string error;
	error = content_file.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto content_refinement_val = obj.GetMember("content");
	if (content_refinement_val.IsValid()) {
		string content_refinement;
		if (json_utils::IsString(content_refinement_val)) {
			content_refinement = json_utils::GetString(content_refinement_val);
		} else {
			return StringUtil::Format(
			    "PositionDeleteFile property 'content_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(content_refinement_val).c_str());
		}
		if (!content_refinement_val.IsNull() && content_refinement != "position-deletes") {
			return "PositionDeleteFile property 'content_refinement' does not match its required const value";
		}
	} else {
		return "PositionDeleteFile required property 'content' is missing";
	}
	auto content_offset_val = obj.GetMember("content-offset");
	if (content_offset_val.IsValid()) {
		int64_t content_offset_tmp;
		if (json_utils::IsInteger(content_offset_val)) {
			content_offset_tmp = json_utils::GetSignedInteger(content_offset_val);
		} else if (json_utils::IsUnsignedInteger(content_offset_val)) {
			content_offset_tmp = json_utils::GetUnsignedInteger(content_offset_val);
		} else {
			return StringUtil::Format(
			    "PositionDeleteFile property 'content_offset_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(content_offset_val).c_str());
		}
		content_offset = std::move(content_offset_tmp);
	}
	auto content_size_in_bytes_val = obj.GetMember("content-size-in-bytes");
	if (content_size_in_bytes_val.IsValid()) {
		int64_t content_size_in_bytes_tmp;
		if (json_utils::IsInteger(content_size_in_bytes_val)) {
			content_size_in_bytes_tmp = json_utils::GetSignedInteger(content_size_in_bytes_val);
		} else if (json_utils::IsUnsignedInteger(content_size_in_bytes_val)) {
			content_size_in_bytes_tmp = json_utils::GetUnsignedInteger(content_size_in_bytes_val);
		} else {
			return StringUtil::Format(
			    "PositionDeleteFile property 'content_size_in_bytes_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(content_size_in_bytes_val).c_str());
		}
		content_size_in_bytes = std::move(content_size_in_bytes_tmp);
	}
	return "";
}

void PositionDeleteFile::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: ContentFile
	content_file.PopulateJSON(writer, obj);

	// Serialize: content-offset
	if (content_offset.has_value()) {
		auto &content_offset_value = *content_offset;
		auto content_offset_json = writer.CreateSignedInteger(content_offset_value);
		obj.Add("content-offset", content_offset_json);
	}

	// Serialize: content-size-in-bytes
	if (content_size_in_bytes.has_value()) {
		auto &content_size_in_bytes_value = *content_size_in_bytes;
		auto content_size_in_bytes_json = writer.CreateSignedInteger(content_size_in_bytes_value);
		obj.Add("content-size-in-bytes", content_size_in_bytes_json);
	}
}

JSONMutableValue PositionDeleteFile::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
