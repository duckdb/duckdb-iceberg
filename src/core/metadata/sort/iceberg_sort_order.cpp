#include "core/metadata/sort/iceberg_sort_order.hpp"
#include "catalog/rest/api/catalog_utils.hpp"

namespace duckdb {

IcebergSortOrderField IcebergSortOrderField::ParseFromJson(const rest_api_objects::SortField &field) {
	IcebergSortOrderField result;
	result.source_id = field.source_id;
	result.transform = field.transform.value;
	result.direction = field.direction.value;
	result.null_order = field.null_order.value;
	return result;
}

IcebergSortOrder IcebergSortOrder::ParseFromJson(const rest_api_objects::SortOrder &sort_order_spec) {
	IcebergSortOrder result;

	result.sort_order_id = sort_order_spec.order_id;
	for (auto &field : sort_order_spec.fields) {
		result.fields.push_back(IcebergSortOrderField::ParseFromJson(field));
	}

	return result;
}

bool IcebergSortOrder::IsSorted() const {
	return !fields.empty();
}

yyjson_mut_val *IcebergSortOrderField::ToJSON(yyjson_mut_doc *doc) const {
	auto res = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_strcpy(doc, res, "transform", transform.RawType().c_str());
	//! FIXME: 'source-ids' (array) if >= V3
	yyjson_mut_obj_add_int(doc, res, "source-id", source_id);
	yyjson_mut_obj_add_strcpy(doc, res, "direction", direction.c_str());
	yyjson_mut_obj_add_strcpy(doc, res, "null-order", null_order.c_str());
	return res;
}

yyjson_mut_val *IcebergSortOrder::ToJSON(yyjson_mut_doc *doc) const {
	auto res = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, res, "order-id", yyjson_mut_int(doc, sort_order_id));
	auto fields_arr = yyjson_mut_arr(doc);
	for (auto &field : fields) {
		yyjson_mut_arr_add_val(fields_arr, field.ToJSON(doc));
	}
	yyjson_mut_obj_add_val(doc, res, "fields", fields_arr);
	return res;
}

} // namespace duckdb
