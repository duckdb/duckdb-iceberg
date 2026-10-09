
#include "rest_catalog/objects/not_predicate.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

NotPredicate::NotPredicate() {
}

NotPredicate NotPredicate::FromJSON(JSONValue obj) {
	NotPredicate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

NotPredicate NotPredicate::Copy() const {
	NotPredicate res;
	res.type = type.Copy();
	res.child = child ? make_uniq<Predicate>(child->Copy()) : nullptr;
	return res;
}

string NotPredicate::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "NotPredicate required property 'type' is missing";
	} else {
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto child_val = obj.GetMember("child");
	if (!child_val.IsValid()) {
		return "NotPredicate required property 'child' is missing";
	} else {
		child = make_uniq<Predicate>();
		error = child->TryFromJSON(child_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void NotPredicate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = type.ToJSON(writer);
	obj.Add("type", type_json);

	// Serialize: child
	auto child_json = child->ToJSON(writer);
	obj.Add("child", child_json);
}

JSONMutableValue NotPredicate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
