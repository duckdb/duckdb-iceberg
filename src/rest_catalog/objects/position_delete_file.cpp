
#include "rest_catalog/objects/position_delete_file.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

PositionDeleteFile::PositionDeleteFile() {
}

PositionDeleteFile PositionDeleteFile::FromJSON(yyjson_val *obj) {
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

string PositionDeleteFile::TryFromJSON(yyjson_val *obj) {
	string error;
	error = content_file.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto content_refinement_val = yyjson_obj_get(obj, "content");
	if (content_refinement_val) {
		string content_refinement;
		if (yyjson_is_str(content_refinement_val)) {
			content_refinement = yyjson_get_str(content_refinement_val);
		} else {
			return StringUtil::Format(
			    "PositionDeleteFile property 'content_refinement' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(content_refinement_val));
		}
		if (!yyjson_is_null(content_refinement_val) && content_refinement != "position-deletes") {
			return "PositionDeleteFile property 'content_refinement' does not match its required const value";
		}
	} else {
		return "PositionDeleteFile required property 'content' is missing";
	}
	auto content_offset_val = yyjson_obj_get(obj, "content-offset");
	if (content_offset_val) {
		int64_t content_offset_tmp;
		if (yyjson_is_sint(content_offset_val)) {
			content_offset_tmp = yyjson_get_sint(content_offset_val);
		} else if (yyjson_is_uint(content_offset_val)) {
			content_offset_tmp = yyjson_get_uint(content_offset_val);
		} else {
			return StringUtil::Format(
			    "PositionDeleteFile property 'content_offset_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(content_offset_val));
		}
		content_offset = std::move(content_offset_tmp);
	}
	auto content_size_in_bytes_val = yyjson_obj_get(obj, "content-size-in-bytes");
	if (content_size_in_bytes_val) {
		int64_t content_size_in_bytes_tmp;
		if (yyjson_is_sint(content_size_in_bytes_val)) {
			content_size_in_bytes_tmp = yyjson_get_sint(content_size_in_bytes_val);
		} else if (yyjson_is_uint(content_size_in_bytes_val)) {
			content_size_in_bytes_tmp = yyjson_get_uint(content_size_in_bytes_val);
		} else {
			return StringUtil::Format(
			    "PositionDeleteFile property 'content_size_in_bytes_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(content_size_in_bytes_val));
		}
		content_size_in_bytes = std::move(content_size_in_bytes_tmp);
	}
	return "";
}

void PositionDeleteFile::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: ContentFile
	content_file.PopulateJSON(doc, obj);

	// Serialize: content-offset
	if (content_offset.has_value()) {
		auto &content_offset_value = *content_offset;
		yyjson_mut_obj_add_sint(doc, obj, "content-offset", content_offset_value);
	}

	// Serialize: content-size-in-bytes
	if (content_size_in_bytes.has_value()) {
		auto &content_size_in_bytes_value = *content_size_in_bytes;
		yyjson_mut_obj_add_sint(doc, obj, "content-size-in-bytes", content_size_in_bytes_value);
	}
}

yyjson_mut_val *PositionDeleteFile::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
