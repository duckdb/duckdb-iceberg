
#include "rest_catalog/objects/comparison_predicate.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ComparisonPredicate::ComparisonPredicate() {
}

ComparisonPredicate ComparisonPredicate::FromJSON(JSONValue obj) {
	ComparisonPredicate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ComparisonPredicate ComparisonPredicate::Copy() const {
	ComparisonPredicate res;
	res.type = type.Copy();
	if (left != nullptr) {
		res.left = left ? make_uniq<ValueExpression>(left->Copy()) : nullptr;
	}
	if (right != nullptr) {
		res.right = right ? make_uniq<ValueExpression>(right->Copy()) : nullptr;
	}
	if (term.has_value()) {
		res.term.emplace();
		(*res.term) = (*term).Copy();
	}
	if (value.has_value()) {
		res.value.emplace();
		(*res.value) = (*value).Copy();
	}
	return res;
}

string ComparisonPredicate::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "ComparisonPredicate required property 'type' is missing";
	} else {
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto left_val = obj.GetMember("left");
	if (left_val.IsValid()) {
		left = make_uniq<ValueExpression>();
		error = left->TryFromJSON(left_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto right_val = obj.GetMember("right");
	if (right_val.IsValid()) {
		right = make_uniq<ValueExpression>();
		error = right->TryFromJSON(right_val);
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
	auto value_val = obj.GetMember("value");
	if (value_val.IsValid()) {
		Literal value_tmp;
		error = value_tmp.TryFromJSON(value_val);
		if (!error.empty()) {
			return error;
		}
		value = std::move(value_tmp);
	}
	return "";
}

void ComparisonPredicate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = type.ToJSON(writer);
	obj.Add("type", type_json);

	// Serialize: left
	if (left != nullptr) {
		auto left_json = left->ToJSON(writer);
		obj.Add("left", left_json);
	}

	// Serialize: right
	if (right != nullptr) {
		auto right_json = right->ToJSON(writer);
		obj.Add("right", right_json);
	}

	// Serialize: term
	if (term.has_value()) {
		auto &term_value = *term;
		auto term_json = term_value.ToJSON(writer);
		obj.Add("term", term_json);
	}

	// Serialize: value
	if (value.has_value()) {
		auto &value_value = *value;
		auto value_json = value_value.ToJSON(writer);
		obj.Add("value", value_json);
	}
}

JSONMutableValue ComparisonPredicate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
