
#include "rest_catalog/objects/transform_term.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

TransformTerm::TransformTerm() {
}

TransformTerm TransformTerm::FromJSON(JSONValue obj) {
	TransformTerm res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

TransformTerm TransformTerm::Copy() const {
	TransformTerm res;
	res.type = type;
	res.transform = transform.Copy();
	res.term = term.Copy();
	return res;
}

string TransformTerm::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "TransformTerm required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("TransformTerm property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "transform") {
			return "TransformTerm property 'type' does not match its required const value";
		}
	}
	auto transform_val = obj.GetMember("transform");
	if (!transform_val.IsValid()) {
		return "TransformTerm required property 'transform' is missing";
	} else {
		error = transform.TryFromJSON(transform_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto term_val = obj.GetMember("term");
	if (!term_val.IsValid()) {
		return "TransformTerm required property 'term' is missing";
	} else {
		error = term.TryFromJSON(term_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void TransformTerm::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: transform
	auto transform_json = transform.ToJSON(writer);
	obj.Add("transform", transform_json);

	// Serialize: term
	auto term_json = term.ToJSON(writer);
	obj.Add("term", term_json);
}

JSONMutableValue TransformTerm::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
