
#include "rest_catalog/objects/and_or_predicate.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

AndOrPredicate::AndOrPredicate() {
}

AndOrPredicate AndOrPredicate::FromJSON(JSONValue obj) {
	AndOrPredicate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AndOrPredicate AndOrPredicate::Copy() const {
	AndOrPredicate res;
	res.type = type.Copy();
	res.left = left ? make_uniq<Predicate>(left->Copy()) : nullptr;
	res.right = right ? make_uniq<Predicate>(right->Copy()) : nullptr;
	return res;
}

string AndOrPredicate::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "AndOrPredicate required property 'type' is missing";
	} else {
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto left_val = obj.GetMember("left");
	if (!left_val.IsValid()) {
		return "AndOrPredicate required property 'left' is missing";
	} else {
		left = make_uniq<Predicate>();
		error = left->TryFromJSON(left_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto right_val = obj.GetMember("right");
	if (!right_val.IsValid()) {
		return "AndOrPredicate required property 'right' is missing";
	} else {
		right = make_uniq<Predicate>();
		error = right->TryFromJSON(right_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void AndOrPredicate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = type.ToJSON(writer);
	obj.Add("type", type_json);

	// Serialize: left
	auto left_json = left->ToJSON(writer);
	obj.Add("left", left_json);

	// Serialize: right
	auto right_json = right->ToJSON(writer);
	obj.Add("right", right_json);
}

JSONMutableValue AndOrPredicate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
