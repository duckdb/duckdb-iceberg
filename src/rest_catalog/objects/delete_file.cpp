
#include "rest_catalog/objects/delete_file.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

DeleteFile::DeleteFile() {
}

DeleteFile DeleteFile::FromJSON(JSONValue obj) {
	DeleteFile res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

DeleteFile DeleteFile::Copy() const {
	DeleteFile res;
	if (position_delete_file.has_value()) {
		res.position_delete_file.emplace();
		(*res.position_delete_file) = (*position_delete_file).Copy();
	}
	if (equality_delete_file.has_value()) {
		res.equality_delete_file.emplace();
		(*res.equality_delete_file) = (*equality_delete_file).Copy();
	}
	return res;
}

string DeleteFile::TryFromJSON(JSONValue obj) {
	string error;
	auto discriminator_val = obj.GetMember("content");
	if (!discriminator_val.IsValid() || !discriminator_val.IsString()) {
		return "DeleteFile discriminator 'content' is missing or is not a string";
	}
	string discriminator = discriminator_val.GetString();
	if (discriminator == "position-deletes") {
		position_delete_file.emplace();
		error = position_delete_file->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "equality-deletes") {
		equality_delete_file.emplace();
		error = equality_delete_file->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else {
		return StringUtil::Format("DeleteFile has unknown discriminator value '%s'", discriminator.c_str());
	}
	return "";
}

void DeleteFile::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	if (position_delete_file.has_value()) {
		position_delete_file->PopulateJSON(writer, obj);
	} else if (equality_delete_file.has_value()) {
		equality_delete_file->PopulateJSON(writer, obj);
	}
}

JSONMutableValue DeleteFile::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
