#include "metadata/iceberg_sort_order.hpp"
#include "catalog_utils.hpp"

namespace duckdb {

IcebergSortOrderField IcebergSortOrderField::ParseFromJson(rest_api_objects::SortField &field) {
	IcebergSortOrderField result;
	result.source_id = field.source_id;
	result.transform = field.transform.value;
	result.direction = field.direction.value;
	result.null_order = field.null_order.value;
	return result;
}

IcebergSortOrder IcebergSortOrder::ParseFromJson(rest_api_objects::SortOrder &sort_order_spec) {
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

} // namespace duckdb
