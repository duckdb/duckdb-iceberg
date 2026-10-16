
#include "rest_catalog/objects/reference.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

Reference::Reference() {
}

Reference Reference::FromJSON(JSONValue obj) {
	Reference res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Reference Reference::Copy() const {
	Reference res;
	if (id_reference.has_value()) {
		res.id_reference.emplace();
		(*res.id_reference) = (*id_reference).Copy();
	}
	if (named_reference.has_value()) {
		res.named_reference.emplace();
		(*res.named_reference) = (*named_reference).Copy();
	}
	return res;
}

string Reference::TryFromJSON(JSONValue obj) {
	string error;
	do {
		id_reference.emplace();
		error = id_reference->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			id_reference = nullopt;
		}
		named_reference.emplace();
		error = named_reference->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			named_reference = nullopt;
		}
		return "Reference failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

void Reference::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	if (id_reference.has_value()) {
		id_reference->PopulateJSON(writer, obj);
	} else if (named_reference.has_value()) {
		named_reference->PopulateJSON(writer, obj);
	}
}

JSONMutableValue Reference::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
