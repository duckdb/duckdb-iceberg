
#include "rest_catalog/objects/sort_field.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

SortField::SortField() {
}

SortField SortField::FromJSON(JSONValue obj) {
	SortField res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SortField SortField::Copy() const {
	SortField res;
	res.source_id = source_id;
	res.transform = transform.Copy();
	res.direction = direction.Copy();
	res.null_order = null_order.Copy();
	return res;
}

string SortField::TryFromJSON(JSONValue obj) {
	string error;
	auto source_id_val = obj.GetMember("source-id");
	if (!source_id_val.IsValid()) {
		return "SortField required property 'source-id' is missing";
	} else {
		if (json_utils::IsInteger(source_id_val)) {
			source_id = json_utils::GetSignedInteger(source_id_val);
		} else {
			return StringUtil::Format("SortField property 'source_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(source_id_val).c_str());
		}
	}
	auto transform_val = obj.GetMember("transform");
	if (!transform_val.IsValid()) {
		return "SortField required property 'transform' is missing";
	} else {
		error = transform.TryFromJSON(transform_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto direction_val = obj.GetMember("direction");
	if (!direction_val.IsValid()) {
		return "SortField required property 'direction' is missing";
	} else {
		error = direction.TryFromJSON(direction_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto null_order_val = obj.GetMember("null-order");
	if (!null_order_val.IsValid()) {
		return "SortField required property 'null-order' is missing";
	} else {
		error = null_order.TryFromJSON(null_order_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void SortField::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: source-id
	auto source_id_json = writer.CreateSignedInteger(source_id);
	obj.Add("source-id", source_id_json);

	// Serialize: transform
	auto transform_json = transform.ToJSON(writer);
	obj.Add("transform", transform_json);

	// Serialize: direction
	auto direction_json = direction.ToJSON(writer);
	obj.Add("direction", direction_json);

	// Serialize: null-order
	auto null_order_json = null_order.ToJSON(writer);
	obj.Add("null-order", null_order_json);
}

JSONMutableValue SortField::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
