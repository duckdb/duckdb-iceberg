
#include "rest_catalog/objects/delete_file.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

DeleteFile::DeleteFile() {
}

DeleteFile DeleteFile::FromJSON(yyjson_val *obj) {
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

string DeleteFile::TryFromJSON(yyjson_val *obj) {
	string error;
	auto discriminator_val = yyjson_obj_get(obj, "content");
	if (!discriminator_val || !yyjson_is_str(discriminator_val)) {
		return "DeleteFile discriminator 'content' is missing or is not a string";
	}
	string discriminator = yyjson_get_str(discriminator_val);
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

void DeleteFile::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	if (position_delete_file.has_value()) {
		position_delete_file->PopulateJSON(doc, obj);
	} else if (equality_delete_file.has_value()) {
		equality_delete_file->PopulateJSON(doc, obj);
	}
}

yyjson_mut_val *DeleteFile::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
