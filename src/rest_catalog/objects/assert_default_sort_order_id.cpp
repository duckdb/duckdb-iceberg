
#include "rest_catalog/objects/assert_default_sort_order_id.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

AssertDefaultSortOrderId::AssertDefaultSortOrderId() {
}

AssertDefaultSortOrderId AssertDefaultSortOrderId::FromJSON(JSONValue obj) {
	AssertDefaultSortOrderId res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AssertDefaultSortOrderId AssertDefaultSortOrderId::Copy() const {
	AssertDefaultSortOrderId res;
	res.type = type;
	res.default_sort_order_id = default_sort_order_id;
	return res;
}

string AssertDefaultSortOrderId::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "AssertDefaultSortOrderId required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format(
			    "AssertDefaultSortOrderId property 'type' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "assert-default-sort-order-id") {
			return "AssertDefaultSortOrderId property 'type' does not match its required const value";
		}
	}
	auto default_sort_order_id_val = obj.GetMember("default-sort-order-id");
	if (!default_sort_order_id_val.IsValid()) {
		return "AssertDefaultSortOrderId required property 'default-sort-order-id' is missing";
	} else {
		if (json_utils::IsInteger(default_sort_order_id_val)) {
			default_sort_order_id = json_utils::GetSignedInteger(default_sort_order_id_val);
		} else {
			return StringUtil::Format(
			    "AssertDefaultSortOrderId property 'default_sort_order_id' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(default_sort_order_id_val).c_str());
		}
	}
	return "";
}

void AssertDefaultSortOrderId::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: default-sort-order-id
	auto default_sort_order_id_json = writer.CreateSignedInteger(default_sort_order_id);
	obj.Add("default-sort-order-id", default_sort_order_id_json);
}

JSONMutableValue AssertDefaultSortOrderId::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
