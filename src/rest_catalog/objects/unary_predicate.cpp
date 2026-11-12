
#include "rest_catalog/objects/unary_predicate.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

UnaryPredicate::UnaryPredicate() {
}

UnaryPredicate UnaryPredicate::FromJSON(JSONValue obj) {
	UnaryPredicate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

UnaryPredicate UnaryPredicate::Copy() const {
	UnaryPredicate res;
	res.type = type.Copy();
	if (child != nullptr) {
		res.child = child ? make_uniq<ValueExpression>(child->Copy()) : nullptr;
	}
	if (term.has_value()) {
		res.term.emplace();
		(*res.term) = (*term).Copy();
	}
	return res;
}

string UnaryPredicate::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "UnaryPredicate required property 'type' is missing";
	} else {
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto child_val = obj.GetMember("child");
	if (child_val.IsValid()) {
		child = make_uniq<ValueExpression>();
		error = child->TryFromJSON(child_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto term_val = obj.GetMember("term");
	if (term_val.IsValid()) {
		Term term_tmp;
		error = term_tmp.TryFromJSON(term_val);
		if (!error.empty()) {
			return error;
		}
		term = std::move(term_tmp);
	}
	return "";
}

void UnaryPredicate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = type.ToJSON(writer);
	obj.Add("type", type_json);

	// Serialize: child
	if (child != nullptr) {
		auto child_json = child->ToJSON(writer);
		obj.Add("child", child_json);
	}

	// Serialize: term
	if (term.has_value()) {
		auto &term_value = *term;
		auto term_json = term_value.ToJSON(writer);
		obj.Add("term", term_json);
	}
}

JSONMutableValue UnaryPredicate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
