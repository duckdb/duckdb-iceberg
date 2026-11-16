
#include "rest_catalog/objects/view_requirement.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ViewRequirement::ViewRequirement() {
}

ViewRequirement ViewRequirement::FromJSON(JSONValue obj) {
	ViewRequirement res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ViewRequirement ViewRequirement::Copy() const {
	ViewRequirement res;
	if (assert_view_uuid.has_value()) {
		res.assert_view_uuid.emplace();
		(*res.assert_view_uuid) = (*assert_view_uuid).Copy();
	}
	return res;
}

string ViewRequirement::TryFromJSON(JSONValue obj) {
	string error;
	auto discriminator_val = obj.GetMember("type");
	if (!discriminator_val.IsValid() || !discriminator_val.IsString()) {
		return "ViewRequirement discriminator 'type' is missing or is not a string";
	}
	string discriminator = discriminator_val.GetString();
	if (discriminator == "assert-view-uuid") {
		assert_view_uuid.emplace();
		error = assert_view_uuid->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else {
		return StringUtil::Format("ViewRequirement has unknown discriminator value '%s'", discriminator.c_str());
	}
	return "";
}

void ViewRequirement::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	if (assert_view_uuid.has_value()) {
		assert_view_uuid->PopulateJSON(writer, obj);
	}
}

JSONMutableValue ViewRequirement::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
