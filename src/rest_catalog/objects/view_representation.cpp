
#include "rest_catalog/objects/view_representation.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ViewRepresentation::ViewRepresentation() {
}

ViewRepresentation ViewRepresentation::FromJSON(JSONValue obj) {
	ViewRepresentation res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ViewRepresentation ViewRepresentation::Copy() const {
	ViewRepresentation res;
	if (sqlview_representation.has_value()) {
		res.sqlview_representation.emplace();
		(*res.sqlview_representation) = (*sqlview_representation).Copy();
	}
	return res;
}

string ViewRepresentation::TryFromJSON(JSONValue obj) {
	string error;
	do {
		sqlview_representation.emplace();
		error = sqlview_representation->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			sqlview_representation = nullopt;
		}
		return "ViewRepresentation failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

void ViewRepresentation::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	if (sqlview_representation.has_value()) {
		sqlview_representation->PopulateJSON(writer, obj);
	}
}

JSONMutableValue ViewRepresentation::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
