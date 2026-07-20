
#include "rest_catalog/objects/assert_default_sort_order_id.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AssertDefaultSortOrderId::AssertDefaultSortOrderId() {
}

AssertDefaultSortOrderId AssertDefaultSortOrderId::FromJSON(yyjson_val *obj) {
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

string AssertDefaultSortOrderId::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "AssertDefaultSortOrderId required property 'type' is missing";
	} else {
		if (yyjson_is_str(type_val)) {
			type = yyjson_get_str(type_val);
		} else {
			return StringUtil::Format(
			    "AssertDefaultSortOrderId property 'type' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(type_val));
		}
		if (!yyjson_is_null(type_val) && type != "assert-default-sort-order-id") {
			return "AssertDefaultSortOrderId property 'type' does not match its required const value";
		}
	}
	auto default_sort_order_id_val = yyjson_obj_get(obj, "default-sort-order-id");
	if (!default_sort_order_id_val) {
		return "AssertDefaultSortOrderId required property 'default-sort-order-id' is missing";
	} else {
		if (yyjson_is_int(default_sort_order_id_val)) {
			default_sort_order_id = yyjson_get_int(default_sort_order_id_val);
		} else {
			return StringUtil::Format("AssertDefaultSortOrderId property 'default_sort_order_id' is not of type "
			                          "'integer', found '%s' instead",
			                          yyjson_get_type_desc(default_sort_order_id_val));
		}
	}
	return "";
}

void AssertDefaultSortOrderId::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: type
	yyjson_mut_obj_add_strcpy(doc, obj, "type", type.c_str());

	// Serialize: default-sort-order-id
	yyjson_mut_obj_add_int(doc, obj, "default-sort-order-id", default_sort_order_id);
}

yyjson_mut_val *AssertDefaultSortOrderId::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
